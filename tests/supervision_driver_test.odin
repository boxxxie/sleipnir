package sleipnir_tests

import "base:intrinsics"
import "core:fmt"
import "core:math/rand"
import "core:mem"
import vmem "core:mem/virtual"
import "core:slice"
import "core:sync"
import "core:testing"
import "core:thread"
import "core:time"
import "../actor"

// =============================================================================
// Layer 3: Supervision Protocol Driver & Test Probes (`tests/supervision_driver_test.odin`)
//
// Translates Layer 2 Scenario DSL verbs into `package actor` runtime calls,
// manages deterministic `Virtual_Clock`, lifecycle trace recording, fault
// injection (`panic` / `fail_turn` / `exit_turn`), monitors, and tenant fixtures.
// =============================================================================

Lifecycle_Event_Kind :: enum u8 {
	Stopped,
	Reinitialized,
}

Lifecycle_Event :: struct {
	label: string,
	kind:  Lifecycle_Event_Kind,
}

Tenant_Gate :: struct {
	entered: sync.Sema,
	release: sync.Sema,
}

Worker_Add :: struct {
	delta: i64,
}

Worker_Commit :: struct {
	new_committed: i64,
}

Worker_Commit_Delta :: struct {
	delta: i64,
}

Worker_Read :: struct {}

Worker_Exit :: struct {
	reason:        actor.Exit_Reason,
	bad_index:     int,
	scratch_bytes: int,
}

Worker_Reply_Then_Panic :: struct {
	reply_val: i64,
	bad_index: int,
}

Worker_Barrier_Panic :: struct {
	barrier: ^sync.Barrier,
}

Worker_Gate_Panic :: struct {
	gate:      ^Tenant_Gate,
	do_panic:  bool,
	bad_index: int,
}

Worker_Fault_With_Hooks :: struct {
	hook_a:        int,
	hook_b:        int,
	scratch_bytes: int,
}

Worker_Delegate_Forward :: struct {
	downstream:   rawptr, // ^Supervised_Test_Worker (broken out of union cycle)
	value:        i64,
	crash_callee: bool,
}

Worker_Delegate_Compute :: struct {
	value:           i64,
	do_crash:        bool,
	forbidden_alloc: rawptr,
}

Worker_Self_Flood_Panic :: struct {
	self_ref: rawptr, // ^Supervised_Test_Worker (broken out of union cycle)
	bad_idx:  int,
}

Test_Worker_Msg :: union {
	Worker_Add,
	Worker_Commit,
	Worker_Commit_Delta,
	Worker_Read,
	Worker_Exit,
	Worker_Reply_Then_Panic,
	Worker_Barrier_Panic,
	Worker_Gate_Panic,
	Worker_Fault_With_Hooks,
	Worker_Delegate_Forward,
	Worker_Delegate_Compute,
	Worker_Self_Flood_Panic,
}

Worker_Slot :: struct {
	world:            ^Supervision_World,
	label:            string,
	committed:        i64,
	fail_reinit_left: int,
	fail_stop_left:   int,
	fail_stop_mode:   actor.Exit_Reason,
}

Abort_Hook_Ctx :: struct {
	world:   ^Supervision_World,
	step_id: int,
}

Test_Worker_State :: struct {
	slot:         ^Worker_Slot,
	live_value:   i64,
	last_scratch: []byte,
}

Supervised_Test_Worker :: actor.Actor(Test_Worker_State, Test_Worker_Msg)

MAX_WATCHER_EVENTS :: 64

Down_Watcher_State :: struct {
	events: [dynamic; MAX_WATCHER_EVENTS]actor.Down_Event,
}

Down_Watcher_Actor :: actor.Actor(Down_Watcher_State, actor.Down_Event)

Supervision_World :: struct {
	t:                 ^testing.T,
	arena:             vmem.Arena,
	sched:             actor.Scheduler,
	clock_ns:          i64,
	next_child_id:     u64,
	supervisors:       map[string]^actor.Supervisor,
	workers:           map[string]^Supervised_Test_Worker,
	worker_ids:        map[string]u64,
	watchers:          map[string]^Down_Watcher_Actor,
	extra_supervisors: [dynamic]^actor.Supervisor,
	extra_workers:     [dynamic]^Supervised_Test_Worker,
	tiny_pools:        [dynamic][]actor.Envelope_Pool(Test_Worker_Msg),
	trace_mu:          sync.Mutex,
	lifecycle_log:     [dynamic]Lifecycle_Event,
	stop_trace:        [dynamic]string,
	reinit_trace:      [dynamic]string,
	abort_hook_log:    [dynamic]int,
	evicted_orgs:      [dynamic]u64,
	tenant_reg:        actor.Registry(u64),
	part_sup:          actor.Partitioned_Supervisor,
	has_part_sup:      bool,
	last_sup_err:      actor.Supervisor_Error,
	last_call_val:     i64,
	last_call_err:     actor.Call_Error,
	last_reg_ok:       bool,
	caller_groups:     map[string][]^Multi_Caller_Actor,
	multi_callers:     map[string]^Multi_Caller_Actor,
	caller_barriers:   [dynamic]^sync.Barrier,
}

// -----------------------------------------------------------------------------
// Deterministic Virtual Clock, Fault Injection & Trace Hooks
// -----------------------------------------------------------------------------

@(private = "file")
world_clock_proc :: proc(user_data: rawptr) -> time.Tick {
	w := cast(^Supervision_World)user_data
	ns := intrinsics.atomic_load_explicit(&w.clock_ns, .Acquire)
	return time.Tick{_nsec = ns}
}

driver_advance_clock :: proc(w: ^Supervision_World, delta: time.Duration) {
	intrinsics.atomic_add_explicit(&w.clock_ns, i64(delta), .Acq_Rel)
}

@(private = "file")
trigger_test_panic :: proc(bad_index: int) {
	// Only execute a real out-of-bounds panic when a `Trap_Frame` is installed (Track 3+).
	// In Track 2 stub mode (`is_trap_active() == false`), avoids crashing the test runner process.
	if actor.is_trap_active() {
		assert(bad_index >= 0 && bad_index < 2, "Index is out of range 0..<2")
	}
}

@(private = "file")
record_lifecycle :: proc(w: ^Supervision_World, label: string, kind: Lifecycle_Event_Kind) {
	if w == nil || len(label) == 0 {
		return
	}
	sync.mutex_lock(&w.trace_mu)
	append(&w.lifecycle_log, Lifecycle_Event{label = label, kind = kind})
	switch kind {
	case .Stopped:
		append(&w.stop_trace, label)
	case .Reinitialized:
		append(&w.reinit_trace, label)
	}
	sync.mutex_unlock(&w.trace_mu)
}

@(private = "file")
record_abort_hook :: proc(raw: rawptr) {
	hctx := cast(^Abort_Hook_Ctx)raw
	sync.mutex_lock(&hctx.world.trace_mu)
	append(&hctx.world.abort_hook_log, hctx.step_id)
	sync.mutex_unlock(&hctx.world.trace_mu)
}

@(private = "file")
test_worker_on_stop :: proc(state: ^Test_Worker_State, reason: actor.Exit_Reason) {
	_ = reason
	if state.slot != nil {
		record_lifecycle(state.slot.world, state.slot.label, .Stopped)
		if state.slot.fail_stop_left > 0 {
			state.slot.fail_stop_left -= 1
			if state.slot.fail_stop_mode == .Error_Return {
				actor.fail_turn(actor.Exit_Reason.Error_Return, "injected on_stop fail_turn")
			} else {
				trigger_test_panic(99)
			}
		}
	}
}

@(private = "file")
test_worker_on_reinit :: proc(state: ^Test_Worker_State, arena_alloc: mem.Allocator) {
	slot := state.slot
	if slot == nil {
		return
	}
	if slot.fail_reinit_left > 0 {
		slot.fail_reinit_left -= 1
		trigger_test_panic(slot.fail_reinit_left + 10)
	}
	state.live_value   = slot.committed
	state.last_scratch = make([]byte, 128, arena_alloc)
	record_lifecycle(slot.world, slot.label, .Reinitialized)
}

@(private = "file")
test_worker_receive :: proc(ctx: ^actor.Context, self_actor: ^Supervised_Test_Worker, msg: ^Test_Worker_Msg) {
	self := &self_actor.state
	switch m in msg^ {
	case Worker_Add:
		self.live_value += m.delta

	case Worker_Commit:
		self.slot.committed = m.new_committed
		self.live_value     = m.new_committed

	case Worker_Commit_Delta:
		self.slot.committed += m.delta
		self.live_value      = self.slot.committed

	case Worker_Read:
		actor.reply(ctx, self.live_value)

	case Worker_Exit:
		if m.scratch_bytes > 0 {
			scratch_alloc := context.allocator if actor.is_trap_active() else context.temp_allocator
			self.last_scratch = make([]byte, m.scratch_bytes, scratch_alloc)
		}
		self.live_value = -999_999
		switch m.reason {
		case .Panic:
			eff_bad := m.bad_index if (m.bad_index < 0 || m.bad_index >= 2) else 99
			trigger_test_panic(eff_bad)
		case .Error_Return:
			actor.fail_turn(i32(7), "requested fail_turn")
		case .Normal, .Supervisor_Shutdown, .Escalated:
			actor.exit_turn(m.reason, "requested worker exit")
		}

	case Worker_Reply_Then_Panic:
		actor.reply(ctx, m.reply_val)
		self.live_value = -999_999
		trigger_test_panic(m.bad_index)

	case Worker_Barrier_Panic:
		sync.barrier_wait(m.barrier)
		trigger_test_panic(99)

	case Worker_Gate_Panic:
		sync.sema_post(&m.gate.entered)
		sync.sema_wait(&m.gate.release)
		if m.do_panic {
			trigger_test_panic(m.bad_index)
		}

	case Worker_Fault_With_Hooks:
		if m.scratch_bytes > 0 {
			scratch_alloc := context.allocator if actor.is_trap_active() else context.temp_allocator
			self.last_scratch = make([]byte, m.scratch_bytes, scratch_alloc)
		}
		arena_alloc := vmem.arena_allocator(&self.slot.world.arena)
		ha := new(Abort_Hook_Ctx, arena_alloc)
		hb := new(Abort_Hook_Ctx, arena_alloc)
		ha^ = Abort_Hook_Ctx{world = self.slot.world, step_id = m.hook_a}
		hb^ = Abort_Hook_Ctx{world = self.slot.world, step_id = m.hook_b}
		actor.defer_on_fault(record_abort_hook, ha)
		actor.defer_on_fault(record_abort_hook, hb)

		self.live_value = -999_999
		trigger_test_panic(77)

	case Worker_Delegate_Forward:
		downstream_actor := cast(^Supervised_Test_Worker)m.downstream
		actor.forward(ctx, downstream_actor, Test_Worker_Msg(Worker_Delegate_Compute{
			value    = m.value,
			do_crash = m.crash_callee,
		}))

	case Worker_Delegate_Compute:
		if m.do_crash {
			trigger_test_panic(88)
			actor.reply(ctx, i64(-1))
			return
		}
		if m.forbidden_alloc != nil && context.allocator.data == m.forbidden_alloc {
			actor.reply(ctx, i64(-999))
			return
		}
		// Exercise both `context.allocator` and `context.temp_allocator` inside the callee turn
		// so any caller-allocator/temp leakage or cross-contamination fails isolation checks.
		buf := make([]byte, 64)
		defer delete(buf)
		buf[0] = byte(m.value & 0xff)
		buf[63] = 0x42
		tmp := fmt.tprintf("callee-%d", m.value)
		_ = tmp
		actor.reply(ctx, m.value * 2)

	case Worker_Self_Flood_Panic:
		if actor.is_trap_active() && m.self_ref != nil {
			self_ref_actor := cast(^Supervised_Test_Worker)m.self_ref
			actor.send(ctx, self_ref_actor, Test_Worker_Msg(Worker_Self_Flood_Panic{
				self_ref = m.self_ref,
				bad_idx  = m.bad_idx,
			}))
		}
		trigger_test_panic(m.bad_idx)
	}
}

// -----------------------------------------------------------------------------
// Driver Operations
// -----------------------------------------------------------------------------

@(require_results)
driver_spawn_supervisor :: proc(
	w: ^Supervision_World,
	strategy: actor.Restart_Strategy,
	max_restarts: int,
	window: time.Duration,
	max_children: int = 0,
) -> ^actor.Supervisor {
	return actor.spawn_supervisor(&w.sched, actor.Supervisor_Config{
		strategy      = strategy,
		max_restarts  = max_restarts,
		max_window    = window,
		within_window = window,
		max_children  = max_children,
		now_proc      = world_clock_proc,
		clock_data    = w,
	})
}

@(require_results)
driver_make_bounded_pools :: proc(
	w: ^Supervision_World,
	slab_capacity: int,
) -> []actor.Envelope_Pool(Test_Worker_Msg) {
	num_pools := w.sched.num_workers + 1
	pools := make([]actor.Envelope_Pool(Test_Worker_Msg), num_pools)
	for i := 0; i < num_pools; i += 1 {
		_ = actor.init(&pools[i], u16(i), slab_capacity, context.allocator, heap_fallback = false)
	}
	append(&w.tiny_pools, pools)
	return pools
}

@(require_results)
driver_spawn_supervised_worker :: proc(
	w: ^Supervision_World,
	sup: ^actor.Supervisor,
	label: string,
	policy: actor.Restart_Policy,
	committed: i64,
	fail_reinit_attempts: int,
	explicit_pools: []actor.Envelope_Pool(Test_Worker_Msg),
	fail_stop_attempts: int = 0,
	fail_stop_mode: actor.Exit_Reason = .Panic,
	use_arena: bool = true,
) -> (worker: ^Supervised_Test_Worker, child_id: u64, err: actor.Supervisor_Error) {
	arena_alloc := vmem.arena_allocator(&w.arena)
	slot := new(Worker_Slot, arena_alloc)
	slot^ = Worker_Slot{
		world            = w,
		label            = label,
		committed        = committed,
		fail_reinit_left = 0,
		fail_stop_left   = fail_stop_attempts,
		fail_stop_mode   = fail_stop_mode,
	}

	init_state := Test_Worker_State{
		slot       = slot,
		live_value = committed,
	}

	if explicit_pools != nil {
		worker, _ = actor.spawn(&w.sched, init_state, test_worker_receive, pools = explicit_pools)
	} else {
		worker, _ = actor.spawn(&w.sched, init_state, test_worker_receive)
	}

	w.next_child_id += 1
	child_id = w.next_child_id
	err = actor.supervise(sup, worker, init_state, policy, actor.Child_Hooks(Test_Worker_State){
		on_reinit    = test_worker_on_reinit,
		on_stop      = test_worker_on_stop,
		use_arena    = use_arena,
		keep_mailbox = false,
	}, child_id)
	slot.fail_reinit_left = fail_reinit_attempts
	return worker, child_id, err
}

@(require_results)
driver_spawn_down_watcher :: proc(w: ^Supervision_World) -> ^Down_Watcher_Actor {
	return actor.spawn(&w.sched, Down_Watcher_State{}, proc(s: ^Down_Watcher_State, ev: actor.Down_Event) {
		append(&s.events, ev)
	})
}

driver_send_worker_and_settle :: proc(
	w: ^Supervision_World,
	worker: ^Supervised_Test_Worker,
	msg: Test_Worker_Msg,
) {
	actor.send(&w.sched, worker, msg)
	actor.wait_idle(&w.sched)
}

driver_send_worker_batch_and_settle :: proc(
	w: ^Supervision_World,
	worker: ^Supervised_Test_Worker,
	msgs: []Test_Worker_Msg,
) {
	if len(worker.pools) > 0 {
		ext_idx := len(worker.pools) - 1
		actor.send(&w.sched, &worker.pools[ext_idx], worker, msgs)
	} else {
		actor.send(&w.sched, worker, msgs)
	}
	actor.wait_idle(&w.sched)
}

@(require_results)
driver_try_read_worker :: proc(
	w: ^Supervision_World,
	label: string,
) -> (i64, actor.Call_Error) {
	worker, ok := w.workers[label]
	if !ok {
		return 0, .Target_Stopped
	}
	return actor.try_call(&w.sched, worker, Test_Worker_Msg(Worker_Read{}), i64)
}

driver_fault_pair_at_barrier :: proc(
	w: ^Supervision_World,
	wa: ^Supervised_Test_Worker,
	wb: ^Supervised_Test_Worker,
) {
	barrier: sync.Barrier
	sync.barrier_init(&barrier, 2)
	actor.send(&w.sched, wa, Test_Worker_Msg(Worker_Barrier_Panic{barrier = &barrier}))
	actor.send(&w.sched, wb, Test_Worker_Msg(Worker_Barrier_Panic{barrier = &barrier}))
	actor.wait_idle(&w.sched)
}

@(private = "file")
Queued_Call_Thread_Ctx :: struct {
	sched:  ^actor.Scheduler,
	target: ^Supervised_Test_Worker,
	err:    actor.Call_Error,
	done:   u32,
}

@(require_results)
driver_verify_queued_call_drains_on_panic :: proc(
	w: ^Supervision_World,
	worker: ^Supervised_Test_Worker,
) -> actor.Call_Error {
	gate: Tenant_Gate
	actor.send(&w.sched, worker, Test_Worker_Msg(Worker_Gate_Panic{
		gate      = &gate,
		do_panic  = true,
		bad_index = 91,
	}))
	sync.sema_wait(&gate.entered)

	tctx := Queued_Call_Thread_Ctx{
		sched  = &w.sched,
		target = worker,
	}
	caller_thread := thread.create(proc(th: ^thread.Thread) {
		c := cast(^Queued_Call_Thread_Ctx)th.data
		_, c.err = actor.try_call(c.sched, c.target, Test_Worker_Msg(Worker_Read{}), i64)
		intrinsics.atomic_store_explicit(&c.done, 1, .Release)
	})
	caller_thread.data = &tctx
	thread.start(caller_thread)

	// Exit early if `try_call` fast-failed/stubbed instead of enqueuing onto `worker.mailbox`.
	for actor.mpsc_is_empty(&worker.mailbox) && intrinsics.atomic_load_explicit(&tctx.done, .Acquire) == 0 {
		intrinsics.cpu_relax()
	}

	sync.sema_post(&gate.release)
	thread.join(caller_thread)
	thread.destroy(caller_thread)
	actor.wait_idle(&w.sched)
	return tctx.err
}

@(private = "file")
Coop_Caller_Req :: struct {
	callee: ^Supervised_Test_Worker,
	tag:    string,
}

@(private = "file")
Coop_Caller_Resp :: struct {
	echoed_tag: string,
	call_err:   actor.Call_Error,
}

@(require_results)
driver_run_cooperative_caller :: proc(
	w: ^Supervision_World,
	callee: ^Supervised_Test_Worker,
	tag: string,
) -> (string, actor.Call_Error) {
	orchestrator := actor.spawn(&w.sched, u8(0), proc(_: ^u8, req: Coop_Caller_Req) {
		scratch := fmt.tprintf("%s", req.tag)
		_, err := actor.try_call(req.callee, Test_Worker_Msg(Worker_Exit{
			reason    = .Panic,
			bad_index = 33,
		}), i64)
		arena_alloc := vmem.arena_allocator(&req.callee.state.slot.world.arena)
		stable_copy := slice.clone(transmute([]byte)scratch, arena_alloc)
		actor.reply(Coop_Caller_Resp{
			echoed_tag = string(stable_copy),
			call_err   = err,
		})
	})

	resp := actor.call(&w.sched, orchestrator, Coop_Caller_Req{callee = callee, tag = tag}, Coop_Caller_Resp)
	actor.wait_idle(&w.sched)
	return resp.echoed_tag, resp.call_err
}

// -----------------------------------------------------------------------------
// Partitioned Multi-Tenant Driver Wiring (`spec_17`, `spec_22`)
// -----------------------------------------------------------------------------

driver_init_partitioned_tenants :: proc(
	w: ^Supervision_World,
	partitions: int,
	idle_timeout: time.Duration,
	max_children: int = 0,
	policy: actor.Restart_Policy = .Permanent,
) {
	actor.registry_init(&w.tenant_reg)
	actor.partitioned_supervisor_init(&w.part_sup, &w.sched, actor.Partitioned_Supervisor_Config{
		num_partitions = partitions,
		max_children   = max_children,
		idle_timeout   = idle_timeout,
		registry       = &w.tenant_reg,
		user_data      = w,
		now_proc       = world_clock_proc,
		clock_data     = w,
		factory        = proc(sched: ^actor.Scheduler, org_id: u64, user_data: rawptr) -> actor.Tenant_Subtree {
			world := cast(^Supervision_World)user_data
			arena_alloc := vmem.arena_allocator(&world.arena)
			slot := new(Worker_Slot, arena_alloc)
			slot^ = Worker_Slot{
				world     = world,
				committed = 100,
			}

			org_sup := actor.spawn_supervisor(sched, actor.Supervisor_Config{
				strategy      = .Rest_For_One,
				max_restarts  = 5,
				max_window    = 10 * time.Second,
				within_window = 10 * time.Second,
				now_proc      = world_clock_proc,
				clock_data    = world,
			})

			init_state := Test_Worker_State{slot = slot, live_value = 100}
			router, _ := actor.spawn(sched, init_state, test_worker_receive, id = u32(org_id))
			_ = actor.supervise(org_sup, router, init_state, .Permanent, actor.Child_Hooks(Test_Worker_State){
				on_reinit    = test_worker_on_reinit,
				on_stop      = test_worker_on_stop,
				use_arena    = true,
				keep_mailbox = false,
			})

			subtree := actor.Tenant_Subtree{
				sup      = org_sup,
				router   = &router.base,
				msg_type = typeid_of(Test_Worker_Msg),
			}
			append(&subtree.children, &router.base)
			return subtree
		},
		on_evict = proc(org_id: u64, subtree: ^actor.Tenant_Subtree, user_data: rawptr) {
			_ = subtree
			world := cast(^Supervision_World)user_data
			sync.mutex_lock(&world.trace_mu)
			append(&world.evicted_orgs, org_id)
			sync.mutex_unlock(&world.trace_mu)
		},
	})
	_ = policy
	w.has_part_sup = true
}

@(require_results)
driver_hold_org_turn :: proc(w: ^Supervision_World, org_id: u64) -> ^Tenant_Gate {
	arena_alloc := vmem.arena_allocator(&w.arena)
	gate := new(Tenant_Gate, arena_alloc)
	_, err := actor.get_or_spawn_tenant(&w.part_sup, org_id, Test_Worker_Msg)
	if err != .None {
		return gate
	}
	actor.send_to_tenant(&w.part_sup, org_id, Test_Worker_Msg(Worker_Gate_Panic{
		gate     = gate,
		do_panic = false,
	}))
	sync.sema_wait(&gate.entered)
	return gate
}

// -----------------------------------------------------------------------------
// Extended Phase 2–4 Drivers (`spec_19`–`spec_28`, `prop_29`)
// -----------------------------------------------------------------------------

driver_build_deep_supervisor_chain :: proc(w: ^Supervision_World, depth: int) -> ^actor.Supervisor {
	root := driver_spawn_supervisor(w, .One_For_One, 5, 10 * time.Second)
	append(&w.extra_supervisors, root)
	parent := root
	for _ in 1 ..< depth {
		child_sup := driver_spawn_supervisor(w, .One_For_One, 5, 10 * time.Second)
		_ = actor.supervise_supervisor(parent, child_sup, .Permanent)
		append(&w.extra_supervisors, child_sup)
		parent = child_sup
	}
	return parent
}

driver_spawn_many_children :: proc(
	w: ^Supervision_World,
	sup: ^actor.Supervisor,
	count: int,
	policy: actor.Restart_Policy,
) {
	for idx in 0 ..< count {
		worker, _, _ := driver_spawn_supervised_worker(w, sup, "", policy, i64(idx), 0, nil)
		append(&w.extra_workers, worker)
	}
}

driver_run_shutdown_crash_race :: proc(w: ^Supervision_World, sup: ^actor.Supervisor, workers: []^Supervised_Test_Worker) {
	for wk in workers {
		actor.send(&w.sched, wk, Test_Worker_Msg(Worker_Self_Flood_Panic{
			self_ref = wk,
			bad_idx  = 77,
		}))
	}
	actor.stop_supervisor(sup)
	actor.wait_idle(&w.sched)
}

@(require_results)
driver_fanout_try_calls :: proc(
	w: ^Supervision_World,
	workers: []^Supervised_Test_Worker,
	value: i64,
	crash_idx: int,
	allocator := context.allocator,
) -> ([]i64, []actor.Call_Error) {
	vals := make([]i64, len(workers), allocator)
	errs := make([]actor.Call_Error, len(workers), allocator)
	for wk, idx in workers {
		vals[idx], errs[idx] = actor.try_call(&w.sched, wk, Test_Worker_Msg(Worker_Delegate_Compute{
			value    = value + i64(idx),
			do_crash = (idx == crash_idx),
		}), i64)
	}
	return vals, errs
}

@(require_results)
driver_run_linearizability_seed :: proc(t: ^testing.T, seed: u64, loc := #caller_location) -> bool {
	rng_state := rand.create(seed)
	rng := rand.default_random_generator(&rng_state)

	w: Supervision_World
	world_init(&w, t, num_workers = 4, loc = loc)
	defer world_destroy(&w)

	root := driver_spawn_supervisor(&w, .One_For_One, 500, 10 * time.Second)
	branch_a := driver_spawn_supervisor(&w, .Rest_For_One, 500, 10 * time.Second)
	branch_b := driver_spawn_supervisor(&w, .One_For_All, 500, 10 * time.Second)
	if actor.supervise_supervisor(root, branch_a, .Permanent, 1) != .None {
		return false
	}
	if actor.supervise_supervisor(root, branch_b, .Permanent, 2) != .None {
		return false
	}

	reg: actor.Registry(u64)
	actor.registry_init(&reg)
	defer actor.registry_destroy(&reg)

	watcher := driver_spawn_down_watcher(&w)
	leafs: [4]^Supervised_Test_Worker
	leaf_ids: [4]u64
	for i in 0 ..< 4 {
		sup := branch_a if i < 2 else branch_b
		pol := actor.Restart_Policy.Permanent if i != 3 else actor.Restart_Policy.Transient
		wk, cid, err := driver_spawn_supervised_worker(&w, sup, "", pol, i64(100 + i), 0, nil)
		if err != .None {
			return false
		}
		leafs[i] = wk
		leaf_ids[i] = cid
		if !actor.registry_register(&reg, u64(1000 + i), wk) {
			return false
		}
	}

	mon_refs: [4]actor.Monitor_Ref
	for i in 0 ..< 4 {
		mon_refs[i] = actor.monitor(actor.pid(watcher), leafs[i])
	}

	prev_gens: [4]u32
	for iter in 0 ..< 24 {
		defer free_all(context.temp_allocator)
		idx := rand.int_max(len(leafs), rng)
		target := leafs[idx]
		op := rand.int_max(7, rng)
		switch op {
		case 0:
			actor.send(&w.sched, target, Test_Worker_Msg(Worker_Add{delta = 1}))
		case 1:
			_, cerr := actor.try_call(&w.sched, target, Test_Worker_Msg(Worker_Read{}), i64)
			if cerr != .None && cerr != .Call_Target_Crashed && cerr != .Target_Stopped && cerr != .Stale_Generation {
				return false
			}
		case 2:
			other := leafs[(idx + 1) % len(leafs)]
			_, cerr := actor.try_call(&w.sched, target, Test_Worker_Msg(Worker_Delegate_Forward{
				downstream   = other,
				value        = i64(iter + 1),
				crash_callee = (iter % 5 == 0),
			}), i64)
			if cerr != .None && cerr != .Call_Target_Crashed && cerr != .Target_Stopped && cerr != .Stale_Generation {
				return false
			}
		case 3:
			actor.send(&w.sched, target, Test_Worker_Msg(Worker_Exit{reason = .Panic, bad_index = 99}))
		case 4:
			actor.send(&w.sched, target, Test_Worker_Msg(Worker_Exit{reason = .Error_Return}))
		case 5:
			_ = actor.supervisor_terminate_child(branch_a if idx < 2 else branch_b, leaf_ids[idx])
			_ = actor.supervisor_restart_child(branch_a if idx < 2 else branch_b, leaf_ids[idx])
		case 6:
			_, _ = actor.registry_lookup(&reg, u64(1000 + idx), Test_Worker_Msg)
		}

		cur_gen := actor.actor_generation(target)
		if cur_gen < prev_gens[idx] {
			return false
		}
		prev_gens[idx] = cur_gen
	}

	actor.stop_supervisor(root)
	actor.wait_idle(&w.sched)

	for i in 0 ..< 4 {
		actor.demonitor(mon_refs[i])
		if actor.actor_status(leafs[i]) != .Stopped {
			return false
		}
		if _, alive := actor.registry_lookup(&reg, u64(1000 + i), Test_Worker_Msg); alive {
			return false
		}
	}
	return w.sched.active_actors == 0 && len(watcher.state.events) > 0
}

@(private = "file")
Multi_Caller_Cmd :: struct {
	kind:    u8, // 0 = normal try_call, 1 = crash try_call, 2 = cycle_a, 3 = cycle_b, 4 = barrier_cycle
	service: ^Supervised_Test_Worker,
	peer:    rawptr,
	barrier: ^sync.Barrier,
	value:   i64,
}

Multi_Caller_Trace :: struct {
	normal_hits:     int,
	arena_intact:    bool,
	crash_err:       actor.Call_Error,
	cycle_outer_err: actor.Call_Error,
	cycle_inner_err: actor.Call_Error,
	barrier_err:     actor.Call_Error,
	stop_count:      int,
	reinit_count:    int,
}

Multi_Caller_State :: struct {
	trace:           ^Multi_Caller_Trace,
	normal_hits:     int,
	all_ok:          bool,
	crash_err:       actor.Call_Error,
	cycle_outer_err: actor.Call_Error,
	cycle_inner_err: actor.Call_Error,
	barrier_err:     actor.Call_Error,
}

Multi_Caller_Actor :: actor.Actor(Multi_Caller_State, Multi_Caller_Cmd)

@(private = "file")
multi_caller_on_stop :: proc(state: ^Multi_Caller_State, reason: actor.Exit_Reason) {
	_ = reason
	if state.trace != nil {
		state.trace.stop_count += 1
	}
}

@(private = "file")
multi_caller_on_reinit :: proc(state: ^Multi_Caller_State, arena_alloc: mem.Allocator) {
	_ = arena_alloc
	if state.trace != nil {
		state.trace.reinit_count += 1
	}
}

@(private = "file")
multi_caller_receive :: proc(ctx: ^actor.Context, self_actor: ^Multi_Caller_Actor, msg: ^Multi_Caller_Cmd) {
	st := &self_actor.state
	switch msg.kind {
	case 0:
		entry_alloc_data := context.allocator.data
		buf := make([]byte, 32)
		buf[0] = byte(msg.value & 0xff)
		buf[31] = 0x5a
		scratch := fmt.tprintf("w%d", msg.value)

		val, err := actor.try_call(ctx.sched, msg.service, Test_Worker_Msg(Worker_Delegate_Compute{
			value           = msg.value,
			do_crash        = false,
			forbidden_alloc = entry_alloc_data,
		}), i64)
		intact := entry_alloc_data != nil &&
			context.allocator.data == entry_alloc_data &&
			buf[0] == byte(msg.value & 0xff) &&
			buf[31] == 0x5a &&
			len(scratch) >= 2 &&
			scratch[0] == 'w' &&
			err == .None &&
			val == msg.value * 2
		if !intact {
			st.all_ok = false
			if st.trace != nil {
				st.trace.arena_intact = false
			}
		}
		st.normal_hits += 1
		if st.trace != nil {
			st.trace.normal_hits += 1
		}

	case 1:
		entry_alloc_data := context.allocator.data
		buf := make([]byte, 32)
		buf[0] = 0x3c
		buf[31] = 0x7e
		scratch := fmt.tprintf("crash-%d", msg.value)

		_, err := actor.try_call(ctx.sched, msg.service, Test_Worker_Msg(Worker_Delegate_Compute{
			value    = msg.value,
			do_crash = true,
		}), i64)
		st.crash_err = err
		intact := entry_alloc_data != nil &&
			context.allocator.data == entry_alloc_data &&
			buf[0] == 0x3c &&
			buf[31] == 0x7e &&
			len(scratch) >= 7 &&
			scratch[0] == 'c'
		if !intact {
			st.all_ok = false
		}
		if st.trace != nil {
			st.trace.crash_err = err
			st.trace.arena_intact = intact
		}

	case 2:
		peer_b := cast(^Multi_Caller_Actor)msg.peer
		inner_err, outer_err := actor.try_call(ctx.sched, peer_b, Multi_Caller_Cmd{
			kind    = 3,
			service = msg.service,
			peer    = self_actor,
			value   = msg.value,
		}, actor.Call_Error)
		st.cycle_outer_err = outer_err
		st.cycle_inner_err = inner_err
		if st.trace != nil {
			st.trace.cycle_outer_err = outer_err
			st.trace.cycle_inner_err = inner_err
		}

	case 3:
		peer_a := cast(^Multi_Caller_Actor)msg.peer
		_, cerr := actor.try_call(ctx.sched, peer_a, Multi_Caller_Cmd{
			kind    = 0,
			service = msg.service,
			value   = 1,
		}, i64)
		actor.reply(ctx, cerr)

	case 4:
		peer := cast(^Multi_Caller_Actor)msg.peer
		sync.barrier_wait(msg.barrier)
		_, cerr := actor.try_call(ctx.sched, peer, Multi_Caller_Cmd{
			kind    = 0,
			service = msg.service,
			value   = msg.value,
		}, i64)
		st.barrier_err = cerr
		if st.trace != nil {
			st.trace.barrier_err = cerr
		}
	}
}

driver_spawn_supervised_caller :: proc(
	w: ^Supervision_World,
	sup: ^actor.Supervisor,
	policy: actor.Restart_Policy,
	use_arena: bool,
	id_hint: u32 = 0,
) -> (^Multi_Caller_Actor, actor.Supervisor_Error) {
	world_alloc := vmem.arena_allocator(&w.arena)
	trace := new(Multi_Caller_Trace, world_alloc)
	trace.arena_intact = true
	init_st := Multi_Caller_State{
		trace  = trace,
		all_ok = true,
	}
	c, _ := actor.spawn(&w.sched, init_st, multi_caller_receive, id_hint)
	w.next_child_id += 1
	sup_err := actor.supervise(sup, c, init_st, policy, actor.Child_Hooks(Multi_Caller_State){
		on_reinit    = multi_caller_on_reinit,
		on_stop      = multi_caller_on_stop,
		use_arena    = use_arena,
		keep_mailbox = false,
	}, w.next_child_id)
	return c, sup_err
}

driver_send_concurrent_try_calls :: proc(
	w: ^Supervision_World,
	callers: []^Multi_Caller_Actor,
	service: ^Supervised_Test_Worker,
	per_caller: int,
) {
	for m in 0 ..< per_caller {
		for c, idx in callers {
			actor.send(&w.sched, c, Multi_Caller_Cmd{
				kind    = 0,
				service = service,
				value   = i64(idx * 100 + m + 1),
			}, shard_hint = idx)
		}
	}
	actor.wait_idle(&w.sched)
}

driver_send_crashing_try_call :: proc(
	w: ^Supervision_World,
	caller: ^Multi_Caller_Actor,
	service: ^Supervised_Test_Worker,
	value: i64,
) {
	actor.send(&w.sched, caller, Multi_Caller_Cmd{
		kind    = 1,
		service = service,
		value   = value,
	})
	actor.wait_idle(&w.sched)
}

driver_send_cyclic_try_call :: proc(
	w: ^Supervision_World,
	caller_a: ^Multi_Caller_Actor,
	caller_b: ^Multi_Caller_Actor,
	service: ^Supervised_Test_Worker,
	value: i64,
) {
	actor.send(&w.sched, caller_a, Multi_Caller_Cmd{
		kind    = 2,
		service = service,
		peer    = caller_b,
		value   = value,
	})
	actor.wait_idle(&w.sched)
}

driver_send_simultaneous_cross_worker_try_call_cycle :: proc(
	w: ^Supervision_World,
	caller_a: ^Multi_Caller_Actor,
	caller_b: ^Multi_Caller_Actor,
	service: ^Supervised_Test_Worker,
) {
	bar := new(sync.Barrier)
	sync.barrier_init(bar, 2)
	append(&w.caller_barriers, bar)
	actor.send(&w.sched, caller_a, Multi_Caller_Cmd{
		kind    = 4,
		service = service,
		peer    = caller_b,
		barrier = bar,
		value   = 1,
	}, shard_hint = 0)
	actor.send(&w.sched, caller_b, Multi_Caller_Cmd{
		kind    = 4,
		service = service,
		peer    = caller_a,
		barrier = bar,
		value   = 2,
	}, shard_hint = 1)
	actor.wait_idle(&w.sched)
}
