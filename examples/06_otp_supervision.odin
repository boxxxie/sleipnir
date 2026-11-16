// =============================================================================
// Example 06: Multi-Tenant OTP Supervision (`Partitioned_Supervisor` + `.Rest_For_One`)
//
// Demonstrates:
//   1. Sharded `Partitioned_Supervisor` managing 50 isolated organizations
//      (each with its own `.Rest_For_One` `Supervisor` and 3-stage pipeline:
//      `Validator -> Enricher -> Writer` = 200 active actors total).
//   2. Thread-safe `Registry(u64)`, `actor.try_call_tenant`, `actor.send_to_tenant`,
//      zero-copy `actor.forward`, and `actor.current_generation()`.
//   3. Out-of-bounds panic recovery via `Trap_Frame` + `vmem.Arena` reset,
//      `try_call` returning `.Call_Target_Crashed`, and `.Rest_For_One`
//      reverse-stop / forward-reinit cascade isolated strictly to `Org_1001`.
//   4. Deterministic idle tenant eviction (`sweep_idle_tenants`) with `on_evict`
//      flush callbacks and `mem.Tracking_Allocator` confirming zero memory leaks.
//
// Run:
//   odin run examples/06_otp_supervision.odin -file -o:speed
// =============================================================================

package main

import "../actor"
import "base:intrinsics"
import "core:fmt"
import "core:mem"
import "core:sync"
import "core:time"

NUM_ORGS :: 50
BASE_ORG_ID :: u64(1001)

// -----------------------------------------------------------------------------
// 1. Durable Per-Tenant Store & Deterministic Virtual Clock
// -----------------------------------------------------------------------------

Org_Store :: struct {
	org_id:           u64,
	committed_total:  i64,
	writes_committed: int,
	flushed_to_cloud: bool,
}

Demo_Env :: struct {
	clock_ns:     i64,
	stores:       [NUM_ORGS]Org_Store,
	trace_mu:     sync.Mutex,
	org_1001_log: [dynamic]string,
}

demo_clock_proc :: proc(user_data: rawptr) -> time.Tick {
	env := cast(^Demo_Env)user_data
	ns := intrinsics.atomic_load_explicit(&env.clock_ns, .Acquire)
	return time.Tick{_nsec = ns}
}

advance_demo_clock :: proc(env: ^Demo_Env, delta: time.Duration) {
	intrinsics.atomic_add_explicit(&env.clock_ns, i64(delta), .Acq_Rel)
}

record_org_1001_event :: proc(env: ^Demo_Env, event: string) {
	sync.mutex_lock(&env.trace_mu)
	append(&env.org_1001_log, event)
	sync.mutex_unlock(&env.trace_mu)
}

// -----------------------------------------------------------------------------
// 2. 3-Stage Pipeline Messages & Worker States
// -----------------------------------------------------------------------------

Pipeline_Process :: struct {
	amount:      i64,
	bonus:       i64,
	force_panic: bool,
	oob_index:   int,
}

Pipeline_Enrich :: struct {
	amount: i64,
	bonus:  i64,
}

Pipeline_Commit :: struct {
	amount: i64,
}

Pipeline_Query_Status :: struct {
	validator_gen: u32,
	enricher_gen:  u32,
}

Pipeline_Status :: struct {
	org_id:          u64,
	committed_total: i64,
	validator_gen:   u32,
	enricher_gen:    u32,
	writer_gen:      u32,
}

Pipeline_Msg :: union {
	Pipeline_Process,
	Pipeline_Enrich,
	Pipeline_Commit,
	Pipeline_Query_Status,
}

Pipeline_Actor :: actor.Actor(Pipeline_Worker_State, Pipeline_Msg)

Stage_Kind :: enum u8 {
	Validator = 0,
	Enricher  = 1,
	Writer    = 2,
}

Pipeline_Worker_State :: struct {
	env:        ^Demo_Env,
	org_id:     u64,
	stage:      Stage_Kind,
	store:      ^Org_Store,
	next_stage: actor.PID(Pipeline_Msg),
	turns:      int,
	scratch:    []byte,
}

pipeline_on_stop :: proc(state: ^Pipeline_Worker_State, reason: actor.Exit_Reason) {
	_ = reason
	if state.org_id == BASE_ORG_ID {
		switch state.stage {
		case .Validator: record_org_1001_event(state.env, "stop:Validator")
		case .Enricher:  record_org_1001_event(state.env, "stop:Enricher")
		case .Writer:    record_org_1001_event(state.env, "stop:Writer")
		}
	}
}

pipeline_on_reinit :: proc(state: ^Pipeline_Worker_State, arena_alloc: actor.Allocator) {
	state.scratch = make([]byte, 256, arena_alloc)
	if state.org_id == BASE_ORG_ID {
		switch state.stage {
		case .Validator: record_org_1001_event(state.env, "reinit:Validator")
		case .Enricher:  record_org_1001_event(state.env, "reinit:Enricher")
		case .Writer:    record_org_1001_event(state.env, "reinit:Writer")
		}
	}
}

validator_receive :: proc(ctx: ^actor.Context, self: ^Pipeline_Actor, msg: ^Pipeline_Msg) {
	s := &self.state
	#partial switch m in msg^ {
	case Pipeline_Process:
		s.turns += 1
		// Allocate temporary per-turn scratch in this actor's `vmem.Arena` (`context.allocator`)
		s.scratch = make([]byte, 1024)
		if m.force_panic {
			header := [2]u8{0xAA, 0xBB}
			assert(m.oob_index >= 0 && m.oob_index < len(header), "validator payload index out of range")
			_ = header[m.oob_index]
		}
		actor.forward(ctx, s.next_stage, Pipeline_Msg(Pipeline_Enrich{
			amount = m.amount,
			bonus  = m.bonus,
		}))
	case Pipeline_Query_Status:
		actor.forward(ctx, s.next_stage, Pipeline_Msg(Pipeline_Query_Status{
			validator_gen = actor.current_generation(),
		}))
	}
}

enricher_receive :: proc(ctx: ^actor.Context, self: ^Pipeline_Actor, msg: ^Pipeline_Msg) {
	s := &self.state
	#partial switch m in msg^ {
	case Pipeline_Enrich:
		s.turns += 1
		s.scratch = make([]byte, 512)
		actor.forward(ctx, s.next_stage, Pipeline_Msg(Pipeline_Commit{
			amount = m.amount + m.bonus,
		}))
	case Pipeline_Query_Status:
		actor.forward(ctx, s.next_stage, Pipeline_Msg(Pipeline_Query_Status{
			validator_gen = m.validator_gen,
			enricher_gen  = actor.current_generation(),
		}))
	}
}

writer_receive :: proc(ctx: ^actor.Context, self: ^Pipeline_Actor, msg: ^Pipeline_Msg) {
	s := &self.state
	#partial switch m in msg^ {
	case Pipeline_Commit:
		s.turns += 1
		s.store.committed_total += m.amount
		s.store.writes_committed += 1
		actor.reply(ctx, Pipeline_Status{
			org_id          = s.org_id,
			committed_total = s.store.committed_total,
			writer_gen      = actor.current_generation(),
		})
	case Pipeline_Query_Status:
		actor.reply(ctx, Pipeline_Status{
			org_id          = s.org_id,
			committed_total = s.store.committed_total,
			validator_gen   = m.validator_gen,
			enricher_gen    = m.enricher_gen,
			writer_gen      = actor.current_generation(),
		})
	}
}

// -----------------------------------------------------------------------------
// 3. Multi-Tenant Factory & Showcase Runner
// -----------------------------------------------------------------------------

tenant_factory :: proc(sched: ^actor.Scheduler, org_id: u64, user_data: rawptr) -> actor.Tenant_Subtree {
	env := cast(^Demo_Env)user_data
	idx := int(org_id - BASE_ORG_ID)
	store := &env.stores[idx]
	store^ = Org_Store{
		org_id          = org_id,
		committed_total = 100,
	}

	org_sup := actor.spawn_supervisor(sched, actor.Supervisor_Config{
		strategy      = .Rest_For_One,
		max_restarts  = 5,
		within_window = 10 * time.Second,
		now_proc      = demo_clock_proc,
		clock_data    = env,
	})

	// Spawn pipeline stages in reverse dependency order so each receives its downstream PID,
	// then supervise in forward pipeline order: [0] Validator -> [1] Enricher -> [2] Writer.
	writer_init := Pipeline_Worker_State{
		env    = env,
		org_id = org_id,
		stage  = .Writer,
		store  = store,
	}
	writer, _ := actor.spawn(sched, writer_init, writer_receive, id = u32(org_id * 10 + 2))

	enricher_init := Pipeline_Worker_State{
		env        = env,
		org_id     = org_id,
		stage      = .Enricher,
		store      = store,
		next_stage = actor.PID(Pipeline_Msg){ref = &writer.ref},
	}
	enricher, _ := actor.spawn(sched, enricher_init, enricher_receive, id = u32(org_id * 10 + 1))

	validator_init := Pipeline_Worker_State{
		env        = env,
		org_id     = org_id,
		stage      = .Validator,
		store      = store,
		next_stage = actor.PID(Pipeline_Msg){ref = &enricher.ref},
	}
	validator, _ := actor.spawn(sched, validator_init, validator_receive, id = u32(org_id * 10))

	hooks := actor.Child_Hooks(Pipeline_Worker_State){
		on_reinit    = pipeline_on_reinit,
		on_stop      = pipeline_on_stop,
		use_arena    = true,
		keep_mailbox = false,
	}
	_ = actor.supervise(org_sup, validator, validator_init, .Permanent, hooks, 1)
	_ = actor.supervise(org_sup, enricher, enricher_init, .Permanent, hooks, 2)
	_ = actor.supervise(org_sup, writer, writer_init, .Permanent, hooks, 3)

	subtree := actor.Tenant_Subtree{
		sup      = org_sup,
		router   = &validator.base,
		msg_type = typeid_of(Pipeline_Msg),
		user_ctx = store,
	}
	append(&subtree.children, &validator.base)
	append(&subtree.children, &enricher.base)
	append(&subtree.children, &writer.base)
	return subtree
}

tenant_evict :: proc(org_id: u64, subtree: ^actor.Tenant_Subtree, user_data: rawptr) {
	_ = org_id
	_ = user_data
	store := cast(^Org_Store)subtree.user_ctx
	if store != nil {
		store.flushed_to_cloud = true
	}
}

run_demo :: proc() {
	env: Demo_Env
	env.clock_ns = i64(time.Second)
	env.org_1001_log = make([dynamic]string, 0, 16)
	defer delete(env.org_1001_log)

	sched := actor.scoped_scheduler(num_workers = 4)

	reg: actor.Registry(u64)
	actor.registry_init(&reg)
	defer actor.registry_destroy(&reg)

	ps: actor.Partitioned_Supervisor
	actor.partitioned_supervisor_init(&ps, sched, actor.Partitioned_Supervisor_Config{
		num_partitions = 16,
		idle_timeout   = 60 * time.Second,
		factory        = tenant_factory,
		on_evict       = tenant_evict,
		user_data      = &env,
		registry       = &reg,
		now_proc       = demo_clock_proc,
		clock_data     = &env,
	})
	defer actor.partitioned_supervisor_destroy(&ps)

	// 1. Spawn and process work across 50 organizations (150 pipeline workers + 50 supervisors = 200 actors)
	fmt.println("--- Step 1: Normal Multi-Tenant Processing Across 50 Organizations ---")
	for i in 0 ..< NUM_ORGS {
		org_id := BASE_ORG_ID + u64(i)
		st, err := actor.try_call_tenant(&ps, org_id, Pipeline_Msg(Pipeline_Process{
			amount = 25,
			bonus  = i64(i),
		}), Pipeline_Status)
		assert(err == .None)
		assert(st.committed_total == 125 + i64(i))
	}
	assert(actor.tenant_count(&ps) == NUM_ORGS)
	fmt.printfln("  Active tenant subtrees: %d (200 supervised actors running)", actor.tenant_count(&ps))

	// 2. Inject out-of-bounds slice panic into Org_1001's Validator (Stage 0):
	//    - `try_call_tenant` cleanly returns `.Call_Target_Crashed`
	//    - `.Rest_For_One` stops Writer -> Enricher in reverse order, then restarts
	//      Validator -> Enricher -> Writer in forward order to generation 2!
	fmt.println("\n--- Step 2: Injecting Panic in Org_1001 Validator (.Rest_For_One Cascade) ---")
	_, crash_err := actor.try_call_tenant(&ps, BASE_ORG_ID, Pipeline_Msg(Pipeline_Process{
		amount      = 999,
		force_panic = true,
		oob_index   = 99,
	}), Pipeline_Status)
	assert(crash_err == .Call_Target_Crashed)
	fmt.printfln("  try_call_tenant(Org_1001) observed expected error: %v", crash_err)

	// Wait for Org_1001's supervisor restart cycle to bring all 3 stages to generation 2
	st_1001: Pipeline_Status
	for _ in 0 ..< 1000 {
		actor.wait_idle(sched)
		q_st, q_err := actor.try_call_tenant(&ps, BASE_ORG_ID, Pipeline_Msg(Pipeline_Query_Status{}), Pipeline_Status)
		if q_err == .None && q_st.validator_gen == 2 && q_st.enricher_gen == 2 && q_st.writer_gen == 2 {
			st_1001 = q_st
			break
		}
		time.sleep(100 * time.Microsecond)
	}
	assert(st_1001.validator_gen == 2 && st_1001.enricher_gen == 2 && st_1001.writer_gen == 2)
	assert(st_1001.committed_total == 125)

	// Verify Org_1001's committed state continues cleanly and peer Org_1050 stayed at generation 1
	actor.send_to_tenant(&ps, BASE_ORG_ID, Pipeline_Msg(Pipeline_Process{amount = 50, bonus = 5}))
	actor.wait_idle(sched)
	err_1001: actor.Call_Error
	st_1001, err_1001 = actor.try_call_tenant(&ps, BASE_ORG_ID, Pipeline_Msg(Pipeline_Query_Status{}), Pipeline_Status)
	st_1050, err_1050 := actor.try_call_tenant(&ps, BASE_ORG_ID + 49, Pipeline_Msg(Pipeline_Query_Status{}), Pipeline_Status)
	assert(err_1001 == .None && err_1050 == .None)
	assert(st_1001.committed_total == 180)
	assert(st_1050.validator_gen == 1 && st_1050.enricher_gen == 1 && st_1050.writer_gen == 1)

	sync.mutex_lock(&env.trace_mu)
	fmt.printfln("  Org_1001 lifecycle trace: %v", env.org_1001_log[:])
	assert(len(env.org_1001_log) == 5)
	assert(env.org_1001_log[0] == "stop:Writer")
	assert(env.org_1001_log[1] == "stop:Enricher")
	assert(env.org_1001_log[2] == "reinit:Validator")
	assert(env.org_1001_log[3] == "reinit:Enricher")
	assert(env.org_1001_log[4] == "reinit:Writer")
	sync.mutex_unlock(&env.trace_mu)
	fmt.printfln("  Org_1001 post-recovery: total=%d gens=(%d,%d,%d) | Org_1050 gens=(%d,%d,%d)",
		st_1001.committed_total, st_1001.validator_gen, st_1001.enricher_gen, st_1001.writer_gen,
		st_1050.validator_gen, st_1050.enricher_gen, st_1050.writer_gen)

	// 3. Two-phase idle eviction: keep Org_1050 active while evicting Org_1001..1049
	fmt.println("\n--- Step 3: Two-Phase Idle Tenant Eviction ---")
	advance_demo_clock(&env, 40 * time.Second)
	_, touch_err := actor.try_call_tenant(&ps, BASE_ORG_ID + 49, Pipeline_Msg(Pipeline_Query_Status{}), Pipeline_Status)
	assert(touch_err == .None)

	advance_demo_clock(&env, 30 * time.Second)
	evicted_first := actor.sweep_idle_tenants(&ps)
	assert(evicted_first == NUM_ORGS - 1)
	assert(actor.tenant_count(&ps) == 1)
	_, ok_1001 := actor.registry_lookup(&reg, BASE_ORG_ID, Pipeline_Msg)
	_, ok_1050 := actor.registry_lookup(&reg, BASE_ORG_ID + 49, Pipeline_Msg)
	assert(!ok_1001 && ok_1050)
	assert(env.stores[0].flushed_to_cloud)
	fmt.printfln("  Evicted %d idle tenants; remaining active=%d (Org_1050 registered=%v)",
		evicted_first, actor.tenant_count(&ps), ok_1050)

	advance_demo_clock(&env, 65 * time.Second)
	evicted_rest := actor.sweep_idle_tenants(&ps)
	assert(evicted_rest == 1)
	assert(actor.tenant_count(&ps) == 0)
	fmt.printfln("  Final sweep evicted %d tenant(s); active=%d", evicted_rest, actor.tenant_count(&ps))
}

main :: proc() {
	track: mem.Tracking_Allocator
	mem.tracking_allocator_init(&track, context.allocator)
	defer mem.tracking_allocator_destroy(&track)

	saved := context.allocator
	context.allocator = mem.tracking_allocator(&track)
	run_demo()
	context.allocator = saved
	free_all(context.temp_allocator)

	fmt.printfln("\nMemory leak check: %d leaked allocations, %d bad frees.",
		len(track.allocation_map), len(track.bad_free_array))
	assert(len(track.allocation_map) == 0)
	assert(len(track.bad_free_array) == 0)
}
