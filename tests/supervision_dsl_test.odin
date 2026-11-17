package sleipnir_tests

import "base:intrinsics"
import "core:slice"
import "core:sync"
import "core:testing"
import "core:time"
import vmem "core:mem/virtual"
import "../actor"

// =============================================================================
// Layer 2: Supervision Domain DSL (`tests/supervision_dsl_test.odin`)
//
// In-place `Supervision_World` lifecycle (`world_init` / `world_destroy`) and
// fluent `given_*` / `upon_*` / `when_*` / `viewing_*` / `shows_*` / `then_*`
// verbs forwarding `loc := #caller_location` so failures pinpoint Layer 1 lines.
// =============================================================================

world_init :: proc(
	w: ^Supervision_World,
	t: ^testing.T,
	num_workers: int = 2,
	loc := #caller_location,
) {
	w.t        = t
	w.clock_ns = i64(time.Second)

	arena_err := vmem.arena_init_growing(&w.arena)
	testing.expect_value(t, arena_err, nil, loc = loc)

	sched_err := actor.init(&w.sched, actor.Scheduler_Config{
		num_workers    = num_workers,
		num_shards     = num_workers,
		enable_parking = true,
	})
	testing.expect_value(t, sched_err, nil, loc = loc)
	actor.scheduler_start(&w.sched)

	w.supervisors       = make(map[string]^actor.Supervisor, 8)
	w.workers           = make(map[string]^Supervised_Test_Worker, 16)
	w.worker_ids        = make(map[string]u64, 16)
	w.watchers          = make(map[string]^Down_Watcher_Actor, 8)
	w.extra_supervisors = make([dynamic]^actor.Supervisor, 0, 16)
	w.extra_workers     = make([dynamic]^Supervised_Test_Worker, 0, 16)
	w.tiny_pools        = make([dynamic][]actor.Envelope_Pool(Test_Worker_Msg), 0, 4)
	w.lifecycle_log     = make([dynamic]Lifecycle_Event, 0, 16)
	w.stop_trace        = make([dynamic]string, 0, 16)
	w.reinit_trace      = make([dynamic]string, 0, 16)
	w.abort_hook_log    = make([dynamic]int, 0, 8)
	w.evicted_orgs      = make([dynamic]u64, 0, 8)
}

world_destroy :: proc(w: ^Supervision_World) {
	actor.wait_idle(&w.sched)
	if w.has_part_sup {
		actor.partitioned_supervisor_destroy(&w.part_sup)
		actor.registry_destroy(&w.tenant_reg)
		w.has_part_sup = false
	}
	actor.destroy(&w.sched)

	for pool_slice in w.tiny_pools {
		for i := 0; i < len(pool_slice); i += 1 {
			actor.destroy(&pool_slice[i])
		}
		delete(pool_slice)
	}
	delete(w.tiny_pools)
	delete(w.evicted_orgs)
	delete(w.abort_hook_log)
	delete(w.reinit_trace)
	delete(w.stop_trace)
	delete(w.lifecycle_log)
	delete(w.extra_workers)
	delete(w.extra_supervisors)
	delete(w.watchers)
	delete(w.worker_ids)
	delete(w.workers)
	delete(w.supervisors)
	vmem.arena_destroy(&w.arena)
	w^ = {}
}

// -----------------------------------------------------------------------------
// `given_*` Precondition Builders
// -----------------------------------------------------------------------------

given_supervisor_window :: proc(
	w: ^Supervision_World,
	label: string,
	strategy: actor.Restart_Strategy,
	max_restarts: int,
	window: time.Duration = 10 * time.Second,
	max_children: int = 0,
	loc := #caller_location,
) {
	sup := driver_spawn_supervisor(w, strategy, max_restarts, window, max_children)
	if !testing.expectf(w.t, sup != nil, "failed to spawn supervisor %q", label, loc = loc) {
		return
	}
	w.supervisors[label] = sup
}

given_supervisor :: proc {
	given_supervisor_window,
}

given_child_supervisor :: proc(
	w: ^Supervision_World,
	label: string,
	under: string,
	strategy: actor.Restart_Strategy,
	max_restarts: int,
	window: time.Duration = 10 * time.Second,
	loc := #caller_location,
) {
	parent, ok := w.supervisors[under]
	if !testing.expectf(w.t, ok, "unknown parent supervisor %q", under, loc = loc) {
		return
	}
	child_sup := driver_spawn_supervisor(w, strategy, max_restarts, window)
	w.next_child_id += 1
	w.last_sup_err = actor.supervise_supervisor(parent, child_sup, .Permanent, w.next_child_id)
	w.supervisors[label] = child_sup
}

given_supervised_worker :: proc(
	w: ^Supervision_World,
	label: string,
	under: string,
	policy: actor.Restart_Policy,
	committed: i64,
	loc := #caller_location,
) {
	sup, ok := w.supervisors[under]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", under, loc = loc) {
		return
	}
	wk, cid, err := driver_spawn_supervised_worker(w, sup, label, policy, committed, 0, nil)
	w.workers[label] = wk
	w.worker_ids[label] = cid
	w.last_sup_err = err
}

given_flaky_reinit_worker :: proc(
	w: ^Supervision_World,
	label: string,
	under: string,
	fail_reinit_attempts: int,
	committed: i64,
	loc := #caller_location,
) {
	sup, ok := w.supervisors[under]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", under, loc = loc) {
		return
	}
	wk, cid, err := driver_spawn_supervised_worker(
		w, sup, label, .Permanent, committed, fail_reinit_attempts, nil,
	)
	w.workers[label] = wk
	w.worker_ids[label] = cid
	w.last_sup_err = err
}

given_flaky_stop_worker :: proc(
	w: ^Supervision_World,
	label: string,
	under: string,
	policy: actor.Restart_Policy,
	fail_stop_attempts: int,
	fail_stop_mode: actor.Exit_Reason,
	committed: i64,
	loc := #caller_location,
) {
	sup, ok := w.supervisors[under]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", under, loc = loc) {
		return
	}
	wk, cid, err := driver_spawn_supervised_worker(
		w, sup, label, policy, committed, 0, nil, fail_stop_attempts, fail_stop_mode,
	)
	w.workers[label] = wk
	w.worker_ids[label] = cid
	w.last_sup_err = err
}

given_tiny_pool_worker :: proc(
	w: ^Supervision_World,
	label: string,
	under: string,
	slab_capacity: int,
	committed: i64,
	loc := #caller_location,
) {
	sup, ok := w.supervisors[under]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", under, loc = loc) {
		return
	}
	pools := driver_make_bounded_pools(w, slab_capacity)
	wk, cid, err := driver_spawn_supervised_worker(w, sup, label, .Permanent, committed, 0, pools)
	w.workers[label] = wk
	w.worker_ids[label] = cid
	w.last_sup_err = err
}

given_down_watcher :: proc(w: ^Supervision_World, label: string) {
	w.watchers[label] = driver_spawn_down_watcher(w)
}

given_partitioned_tenant_supervisor :: proc(
	w: ^Supervision_World,
	partitions: int,
	idle_timeout: time.Duration,
	max_children: int = 0,
	policy: actor.Restart_Policy = .Permanent,
) {
	driver_init_partitioned_tenants(w, partitions, idle_timeout, max_children, policy)
}

given_supervisor_chain :: proc(
	w: ^Supervision_World,
	root_label: string,
	depth: int,
	leaf_sup_label: string,
) {
	leaf_sup := driver_build_deep_supervisor_chain(w, depth)
	w.supervisors[root_label] = w.extra_supervisors[0]
	w.supervisors[leaf_sup_label] = leaf_sup
}

// -----------------------------------------------------------------------------
// `upon_*` / `when_*` Domain Triggers
// -----------------------------------------------------------------------------

upon_adding_uncommitted_delta :: proc(
	w: ^Supervision_World,
	label: string,
	delta: i64,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	driver_send_worker_and_settle(w, worker, Worker_Add{delta = delta})
}

upon_committing_snapshot :: proc(
	w: ^Supervision_World,
	label: string,
	snapshot: i64,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	driver_send_worker_and_settle(w, worker, Worker_Commit{new_committed = snapshot})
}

upon_exiting_worker :: proc(
	w: ^Supervision_World,
	label: string,
	reason: actor.Exit_Reason,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	driver_send_worker_and_settle(w, worker, Worker_Exit{reason = reason, bad_index = 99})
}

upon_triggering_bounds_panic :: proc(
	w: ^Supervision_World,
	label: string,
	bad_index: int,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	driver_send_worker_and_settle(w, worker, Worker_Exit{reason = .Panic, bad_index = bad_index})
}

upon_faulting_workers_simultaneously :: proc(
	w: ^Supervision_World,
	label_a: string,
	label_b: string,
	loc := #caller_location,
) {
	wa, ok_a := w.workers[label_a]
	wb, ok_b := w.workers[label_b]
	if !testing.expectf(w.t, ok_a && ok_b, "unknown worker labels %q / %q", label_a, label_b, loc = loc) {
		return
	}
	driver_fault_pair_at_barrier(w, wa, wb)
}

upon_clearing_lifecycle_log :: proc(w: ^Supervision_World) {
	sync.mutex_lock(&w.trace_mu)
	clear(&w.lifecycle_log)
	clear(&w.stop_trace)
	clear(&w.reinit_trace)
	sync.mutex_unlock(&w.trace_mu)
}

upon_advancing_clock :: proc(w: ^Supervision_World, delta: time.Duration) {
	driver_advance_clock(w, delta)
}

when_advancing_clock_ms :: proc(w: ^Supervision_World, ms: i64) {
	driver_advance_clock(w, time.Duration(ms) * time.Millisecond)
}

@(require_results)
upon_monitoring :: proc(
	w: ^Supervision_World,
	watcher: string,
	target: string,
	loc := #caller_location,
) -> actor.Monitor_Ref {
	wa, ok_w := w.watchers[watcher]
	ta, ok_t := w.workers[target]
	if !testing.expectf(w.t, ok_w && ok_t, "unknown monitor endpoints %q -> %q", watcher, target, loc = loc) {
		return {}
	}
	ref := actor.monitor(actor.pid(wa), ta)
	actor.wait_idle(&w.sched)
	return ref
}

upon_demonitoring :: proc(w: ^Supervision_World, m: actor.Monitor_Ref) {
	actor.demonitor(m)
	actor.wait_idle(&w.sched)
}

upon_sending_4_msg_batch_with_panic_at_3 :: proc(
	w: ^Supervision_World,
	label: string,
	arena_scratch_bytes: int,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	batch := [4]Test_Worker_Msg{
		Worker_Add{delta = 10},
		Worker_Add{delta = 20},
		Worker_Exit{reason = .Panic, bad_index = 55, scratch_bytes = arena_scratch_bytes},
		Worker_Add{delta = 40},
	}
	driver_send_worker_batch_and_settle(w, worker, batch[:])
}

upon_sending_valid_increments :: proc(
	w: ^Supervision_World,
	label: string,
	count: int,
	delta: i64,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	for _ in 0 ..< count {
		driver_send_worker_and_settle(w, worker, Worker_Add{delta = delta})
	}
}

upon_faulting_with_abort_hooks :: proc(
	w: ^Supervision_World,
	label: string,
	hook_a: int,
	hook_b: int,
	arena_scratch_bytes: int,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	driver_send_worker_and_settle(w, worker, Worker_Fault_With_Hooks{
		hook_a        = hook_a,
		hook_b        = hook_b,
		scratch_bytes = arena_scratch_bytes,
	})
}

upon_sending_org_delta :: proc(w: ^Supervision_World, org_id: u64, delta: i64) {
	actor.send_to_tenant(&w.part_sup, org_id, Test_Worker_Msg(Worker_Add{delta = delta}))
	actor.wait_idle(&w.sched)
}

upon_crashing_org_router :: proc(w: ^Supervision_World, org_id: u64, bad_index: int) {
	actor.send_to_tenant(&w.part_sup, org_id, Test_Worker_Msg(Worker_Exit{
		reason    = .Panic,
		bad_index = bad_index,
	}))
	actor.wait_idle(&w.sched)
}

@(require_results)
upon_holding_org_turn_open :: proc(w: ^Supervision_World, org_id: u64) -> ^Tenant_Gate {
	return driver_hold_org_turn(w, org_id)
}

upon_releasing_org_turn :: proc(w: ^Supervision_World, gate: ^Tenant_Gate) {
	sync.sema_post(&gate.release)
	actor.wait_idle(&w.sched)
}

upon_sweeping_idle_tenants :: proc(w: ^Supervision_World) {
	_ = actor.sweep_idle_tenants(&w.part_sup)
}

upon_stopping_supervisor :: proc(w: ^Supervision_World, label: string, loc := #caller_location) {
	sup, ok := w.supervisors[label]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", label, loc = loc) {
		return
	}
	actor.stop_supervisor(sup)
	actor.wait_idle(&w.sched)
}

when_terminating_child :: proc(
	w: ^Supervision_World,
	sup_label: string,
	worker_label: string,
	loc := #caller_location,
) {
	sup, ok_s := w.supervisors[sup_label]
	cid, ok_c := w.worker_ids[worker_label]
	if !testing.expectf(w.t, ok_s && ok_c, "unknown child admin target %q/%q", sup_label, worker_label, loc = loc) {
		return
	}
	w.last_sup_err = actor.supervisor_terminate_child(sup, cid)
	actor.wait_idle(&w.sched)
}

when_restarting_child :: proc(
	w: ^Supervision_World,
	sup_label: string,
	worker_label: string,
	loc := #caller_location,
) {
	sup, ok_s := w.supervisors[sup_label]
	cid, ok_c := w.worker_ids[worker_label]
	if !testing.expectf(w.t, ok_s && ok_c, "unknown child admin target %q/%q", sup_label, worker_label, loc = loc) {
		return
	}
	w.last_sup_err = actor.supervisor_restart_child(sup, cid)
	actor.wait_idle(&w.sched)
}

when_deleting_child :: proc(
	w: ^Supervision_World,
	sup_label: string,
	worker_label: string,
	loc := #caller_location,
) {
	sup, ok_s := w.supervisors[sup_label]
	cid, ok_c := w.worker_ids[worker_label]
	if !testing.expectf(w.t, ok_s && ok_c, "unknown child admin target %q/%q", sup_label, worker_label, loc = loc) {
		return
	}
	w.last_sup_err = actor.supervisor_delete_child(sup, cid)
	actor.wait_idle(&w.sched)
}

when_trying_forwarded_call :: proc(
	w: ^Supervision_World,
	router_label: string,
	worker_label: string,
	value: i64,
	crash_callee: bool,
	loc := #caller_location,
) {
	router, ok_r := w.workers[router_label]
	worker, ok_w := w.workers[worker_label]
	if !testing.expectf(w.t, ok_r && ok_w, "unknown forward endpoints %q -> %q", router_label, worker_label, loc = loc) {
		return
	}
	w.last_call_val, w.last_call_err = actor.try_call(&w.sched, router, Test_Worker_Msg(Worker_Delegate_Forward{
		downstream   = worker,
		value        = value,
		crash_callee = crash_callee,
	}), i64)
	actor.wait_idle(&w.sched)
}

when_spawning_excess_worker :: proc(
	w: ^Supervision_World,
	label: string,
	under: string,
	policy: actor.Restart_Policy,
	committed: i64,
	loc := #caller_location,
) {
	sup, ok := w.supervisors[under]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", under, loc = loc) {
		return
	}
	wk, cid, err := driver_spawn_supervised_worker(w, sup, label, policy, committed, 0, nil)
	w.last_sup_err = err
	if err == .None {
		w.workers[label] = wk
		w.worker_ids[label] = cid
	} else {
		append(&w.extra_workers, wk)
	}
}

// -----------------------------------------------------------------------------
// `viewing_*` / `shows_*` / `then_*` Scoped & Direct Assertions
// -----------------------------------------------------------------------------

@(require_results)
viewing_worker :: proc(
	w: ^Supervision_World,
	label: string,
	loc := #caller_location,
) -> (^Supervised_Test_Worker, bool) {
	worker, ok := w.workers[label]
	testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc)
	return worker, ok
}

shows_generation :: proc(
	w: ^Supervision_World,
	worker: ^Supervised_Test_Worker,
	expected: u32,
	loc := #caller_location,
) {
	testing.expect_value(w.t, actor.actor_generation(worker), expected, loc = loc)
}

shows_value :: proc(
	w: ^Supervision_World,
	worker: ^Supervised_Test_Worker,
	expected: i64,
	loc := #caller_location,
) {
	val, err := actor.try_call(&w.sched, worker, Test_Worker_Msg(Worker_Read{}), i64)
	testing.expect_value(w.t, err, actor.Call_Error.None, loc = loc)
	testing.expect_value(w.t, val, expected, loc = loc)
}

then_worker_status_is :: proc(
	w: ^Supervision_World,
	label: string,
	expected: actor.Actor_Status,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	testing.expect_value(w.t, actor.actor_status(worker), expected, loc = loc)
}

then_supervisor_stats_are :: proc(
	w: ^Supervision_World,
	label: string,
	expected_cycles: int,
	expected_restarts: int,
	loc := #caller_location,
) {
	sup, ok := w.supervisors[label]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", label, loc = loc) {
		return
	}
	stats := actor.supervisor_stats(sup)
	testing.expect_value(w.t, stats.restart_cycles, expected_cycles, loc = loc)
	testing.expect_value(w.t, stats.children_restarted, expected_restarts, loc = loc)
}

then_supervisor_escalated :: proc(
	w: ^Supervision_World,
	label: string,
	expected: bool,
	loc := #caller_location,
) {
	sup, ok := w.supervisors[label]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", label, loc = loc) {
		return
	}
	stats := actor.supervisor_stats(sup)
	testing.expect_value(w.t, stats.escalated, expected, loc = loc)
}

then_supervisor_is_stopped :: proc(w: ^Supervision_World, label: string, loc := #caller_location) {
	sup, ok := w.supervisors[label]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", label, loc = loc) {
		return
	}
	testing.expect_value(w.t, actor.actor_status(sup), actor.Actor_Status.Stopped, loc = loc)
}

then_supervisor_is_running :: proc(w: ^Supervision_World, label: string, loc := #caller_location) {
	sup, ok := w.supervisors[label]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", label, loc = loc) {
		return
	}
	testing.expect_value(w.t, actor.actor_status(sup), actor.Actor_Status.Running, loc = loc)
}

then_lifecycle_events_are :: proc(
	w: ^Supervision_World,
	expected: ..Lifecycle_Event,
	loc := #caller_location,
) {
	sync.mutex_lock(&w.trace_mu)
	actual := slice.clone(w.lifecycle_log[:], context.temp_allocator)
	sync.mutex_unlock(&w.trace_mu)

	if !testing.expect_value(w.t, len(actual), len(expected), loc = loc) {
		return
	}
	for i := 0; i < len(expected); i += 1 {
		testing.expect_value(w.t, actual[i], expected[i], loc = loc)
	}
}

then_stop_trace_is :: proc(w: ^Supervision_World, expected: ..string, loc := #caller_location) {
	sync.mutex_lock(&w.trace_mu)
	actual := slice.clone(w.stop_trace[:], context.temp_allocator)
	sync.mutex_unlock(&w.trace_mu)

	if !testing.expect_value(w.t, len(actual), len(expected), loc = loc) {
		return
	}
	for i := 0; i < len(expected); i += 1 {
		testing.expect_value(w.t, actual[i], expected[i], loc = loc)
	}
}

then_reinit_trace_is :: proc(w: ^Supervision_World, expected: ..string, loc := #caller_location) {
	sync.mutex_lock(&w.trace_mu)
	actual := slice.clone(w.reinit_trace[:], context.temp_allocator)
	sync.mutex_unlock(&w.trace_mu)

	if !testing.expect_value(w.t, len(actual), len(expected), loc = loc) {
		return
	}
	for i := 0; i < len(expected); i += 1 {
		testing.expect_value(w.t, actual[i], expected[i], loc = loc)
	}
}

then_watcher_received_down :: proc(
	w: ^Supervision_World,
	label: string,
	count: int,
	reason: actor.Exit_Reason = .Normal,
	loc := #caller_location,
) {
	watcher, ok := w.watchers[label]
	if !testing.expectf(w.t, ok, "unknown watcher %q", label, loc = loc) {
		return
	}
	actor.wait_idle(&w.sched)
	testing.expect_value(w.t, len(watcher.state.events), count, loc = loc)
	if count > 0 && len(watcher.state.events) == count {
		testing.expect_value(w.t, watcher.state.events[count - 1].reason, reason, loc = loc)
	}
}

then_sync_call_fails_on_active_panic :: proc(
	w: ^Supervision_World,
	label: string,
	expected_err: actor.Call_Error,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	_, err := actor.try_call(&w.sched, worker, Test_Worker_Msg(Worker_Exit{
		reason    = .Panic,
		bad_index = 66,
	}), i64)
	actor.wait_idle(&w.sched)
	testing.expect_value(w.t, err, expected_err, loc = loc)
}

then_queued_sync_call_fails_on_mailbox_drain :: proc(
	w: ^Supervision_World,
	label: string,
	expected_err: actor.Call_Error,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	err := driver_verify_queued_call_drains_on_panic(w, worker)
	testing.expect_value(w.t, err, expected_err, loc = loc)
}

then_sync_call_succeeds_despite_post_reply_panic :: proc(
	w: ^Supervision_World,
	label: string,
	reply_val: i64,
	loc := #caller_location,
) {
	worker, ok := w.workers[label]
	if !testing.expectf(w.t, ok, "unknown worker %q", label, loc = loc) {
		return
	}
	val, err := actor.try_call(&w.sched, worker, Test_Worker_Msg(Worker_Reply_Then_Panic{
		reply_val = reply_val,
		bad_index = 88,
	}), i64)
	actor.wait_idle(&w.sched)
	testing.expect_value(w.t, err, actor.Call_Error.None, loc = loc)
	testing.expect_value(w.t, val, reply_val, loc = loc)
}

then_abort_hooks_ran_in_lifo_order :: proc(
	w: ^Supervision_World,
	expected: ..int,
	loc := #caller_location,
) {
	sync.mutex_lock(&w.trace_mu)
	actual := slice.clone(w.abort_hook_log[:], context.temp_allocator)
	sync.mutex_unlock(&w.trace_mu)

	testing.expect(w.t, slice.equal(actual, expected), loc = loc)
}

then_cooperative_caller_preserves_temp_scratch_on_callee_panic :: proc(
	w: ^Supervision_World,
	callee_label: string,
	scratch_tag: string,
	expected_err: actor.Call_Error,
	loc := #caller_location,
) {
	callee, ok := w.workers[callee_label]
	if !testing.expectf(w.t, ok, "unknown callee %q", callee_label, loc = loc) {
		return
	}
	echoed, observed_err := driver_run_cooperative_caller(w, callee, scratch_tag)
	testing.expect_value(w.t, observed_err, expected_err, loc = loc)
	testing.expect_value(w.t, echoed, scratch_tag, loc = loc)
}

then_org_is_registered :: proc(
	w: ^Supervision_World,
	org_id: u64,
	expected: bool,
	loc := #caller_location,
) {
	_, found := actor.registry_lookup(&w.tenant_reg, org_id, Test_Worker_Msg)
	testing.expect_value(w.t, found, expected, loc = loc)
}

then_org_value_is :: proc(
	w: ^Supervision_World,
	org_id: u64,
	expected: i64,
	loc := #caller_location,
) {
	val, err := actor.try_call_tenant(&w.part_sup, org_id, Test_Worker_Msg(Worker_Read{}), i64)
	testing.expect_value(w.t, err, actor.Call_Error.None, loc = loc)
	testing.expect_value(w.t, val, expected, loc = loc)
}

then_evicted_orgs_are :: proc(
	w: ^Supervision_World,
	expected: ..u64,
	loc := #caller_location,
) {
	sync.mutex_lock(&w.trace_mu)
	actual := slice.clone(w.evicted_orgs[:], context.temp_allocator)
	sync.mutex_unlock(&w.trace_mu)

	slice.sort(actual)
	sorted_expected := slice.clone(expected, context.temp_allocator)
	slice.sort(sorted_expected)
	testing.expect(w.t, slice.equal(actual, sorted_expected), loc = loc)
}

then_tenant_count_is :: proc(w: ^Supervision_World, expected: int, loc := #caller_location) {
	testing.expect_value(w.t, actor.tenant_count(&w.part_sup), expected, loc = loc)
}

then_last_supervisor_error_is :: proc(
	w: ^Supervision_World,
	expected: actor.Supervisor_Error,
	loc := #caller_location,
) {
	testing.expect_value(w.t, w.last_sup_err, expected, loc = loc)
}

then_last_call_result_is :: proc(
	w: ^Supervision_World,
	expected_val: i64,
	expected_err: actor.Call_Error,
	loc := #caller_location,
) {
	testing.expect_value(w.t, w.last_call_err, expected_err, loc = loc)
	testing.expect_value(w.t, w.last_call_val, expected_val, loc = loc)
}

then_child_counts_are :: proc(
	w: ^Supervision_World,
	sup_label: string,
	expected: actor.Child_Counts,
	loc := #caller_location,
) {
	sup, ok := w.supervisors[sup_label]
	if !testing.expectf(w.t, ok, "unknown supervisor %q", sup_label, loc = loc) {
		return
	}
	actual := actor.supervisor_count_children(sup)
	testing.expect_value(w.t, actual, expected, loc = loc)
}

then_which_children_excludes :: proc(
	w: ^Supervision_World,
	sup_label: string,
	excluded_label: string,
	loc := #caller_location,
) {
	sup, ok_s := w.supervisors[sup_label]
	cid, ok_c := w.worker_ids[excluded_label]
	if !testing.expectf(w.t, ok_s && ok_c, "unknown supervisor/worker %q/%q", sup_label, excluded_label, loc = loc) {
		return
	}
	infos := actor.supervisor_which_children(sup, context.temp_allocator)
	found := false
	for info in infos {
		if info.id == cid {
			found = true
			break
		}
	}
	testing.expect(w.t, !found, loc = loc)
}

then_no_leaks :: proc(w: ^Supervision_World, loc := #caller_location) {
	_ = intrinsics.atomic_load_explicit(&w.clock_ns, .Relaxed)
	testing.expect(w.t, w.sched.active_actors == 0, loc = loc)
}

then_multi_worker_cooperative_try_calls_succeed_and_isolate_faults :: proc(
	w: ^Supervision_World,
	service_label: string,
	loc := #caller_location,
) {
	service, ok := w.workers[service_label]
	if !testing.expectf(w.t, ok, "unknown service worker %q", service_label, loc = loc) {
		return
	}
	testing.expect(w.t, driver_verify_multi_worker_cooperative_try_call_saturation(w, service), loc = loc)
}
