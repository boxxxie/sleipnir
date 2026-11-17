package sleipnir_tests

import "core:fmt"
import "core:testing"
import "core:time"
import "../actor"

// =============================================================================
// Layer 1: Executable OTP Supervision Specifications (`tests/supervision_spec_test.odin`)
//
// Red-First 4-layer ATDD specifications covering `spec_01`..`spec_28`, `prop_18`,
// and `prop_29`. Every test is wrapped in `with_leak_check` so failures remain
// deterministic, hang-free, crash-free, and zero-leak against Track 2 stubs.
// =============================================================================

@(test)
spec_01_restart_policy_and_exit_reason_matrix :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		Matrix_Case :: struct {
			label:          string,
			policy:         actor.Restart_Policy,
			reason:         actor.Exit_Reason,
			expect_restart: bool,
		}

		cases := [9]Matrix_Case{
			{"perm_normal", .Permanent, .Normal, true},
			{"perm_panic", .Permanent, .Panic, true},
			{"perm_err", .Permanent, .Error_Return, true},
			{"trans_normal", .Transient, .Normal, false},
			{"trans_panic", .Transient, .Panic, true},
			{"trans_err", .Transient, .Error_Return, true},
			{"temp_normal", .Temporary, .Normal, false},
			{"temp_panic", .Temporary, .Panic, false},
			{"temp_err", .Temporary, .Error_Return, false},
		}

		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 20)

		expected_cycles := 0
		expected_restarts := 0
		for tc in cases {
			given_supervised_worker(&w, tc.label, "sup", tc.policy, committed = 100)
			upon_exiting_worker(&w, tc.label, tc.reason)

			wk, ok := viewing_worker(&w, tc.label)
			if ok && tc.expect_restart {
				expected_cycles += 1
				expected_restarts += 1
				then_worker_status_is(&w, tc.label, .Running)
				shows_generation(&w, wk, 2)
				shows_value(&w, wk, 100)
			} else {
				then_worker_status_is(&w, tc.label, .Stopped)
				if ok {
					shows_generation(&w, wk, 1)
				}
			}
			then_supervisor_stats_are(&w, "sup", expected_cycles, expected_restarts)
		}
	})
}

@(test)
spec_02_one_for_one_restart_isolates_siblings :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 3)
		given_supervised_worker(&w, "worker_a", "sup", .Permanent, committed = 10)
		given_supervised_worker(&w, "worker_b", "sup", .Permanent, committed = 200)

		upon_adding_uncommitted_delta(&w, "worker_a", 5)
		upon_adding_uncommitted_delta(&w, "worker_b", 50)
		upon_triggering_bounds_panic(&w, "worker_a", bad_index = 99)

		if a, ok_a := viewing_worker(&w, "worker_a"); ok_a {
			shows_generation(&w, a, 2)
			shows_value(&w, a, 10)
		}
		if b, ok_b := viewing_worker(&w, "worker_b"); ok_b {
			shows_generation(&w, b, 1)
			shows_value(&w, b, 250)
		}
		then_worker_status_is(&w, "worker_a", .Running)
		then_worker_status_is(&w, "worker_b", .Running)
		then_supervisor_stats_are(&w, "sup", expected_cycles = 1, expected_restarts = 1)
	})
}

@(test)
spec_03_rest_for_one_restarts_downstream_siblings_only :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .Rest_For_One, max_restarts = 3)
		given_supervised_worker(&w, "a", "sup", .Permanent, committed = 100)
		given_supervised_worker(&w, "b", "sup", .Permanent, committed = 200)
		given_supervised_worker(&w, "c", "sup", .Permanent, committed = 300)
		given_supervised_worker(&w, "d", "sup", .Permanent, committed = 400)

		upon_adding_uncommitted_delta(&w, "a", 1)
		upon_adding_uncommitted_delta(&w, "b", 2)
		upon_adding_uncommitted_delta(&w, "c", 3)
		upon_adding_uncommitted_delta(&w, "d", 4)
		upon_clearing_lifecycle_log(&w)

		upon_triggering_bounds_panic(&w, "b", bad_index = 5)

		if a, ok_a := viewing_worker(&w, "a"); ok_a {
			shows_generation(&w, a, 1)
			shows_value(&w, a, 101)
		}
		if b, ok_b := viewing_worker(&w, "b"); ok_b {
			shows_generation(&w, b, 2)
			shows_value(&w, b, 200)
		}
		if c, ok_c := viewing_worker(&w, "c"); ok_c {
			shows_generation(&w, c, 2)
			shows_value(&w, c, 300)
		}
		if d, ok_d := viewing_worker(&w, "d"); ok_d {
			shows_generation(&w, d, 2)
			shows_value(&w, d, 400)
		}

		then_stop_trace_is(&w, "d", "c")
		then_reinit_trace_is(&w, "b", "c", "d")
		then_supervisor_stats_are(&w, "sup", expected_cycles = 1, expected_restarts = 3)
	})
}

@(test)
spec_04_one_for_all_stops_all_siblings_in_reverse_order_before_restarting :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_All, max_restarts = 3)
		given_supervised_worker(&w, "a", "sup", .Permanent, committed = 10)
		given_supervised_worker(&w, "b", "sup", .Permanent, committed = 20)
		given_supervised_worker(&w, "c", "sup", .Permanent, committed = 30)

		upon_adding_uncommitted_delta(&w, "a", 5)
		upon_adding_uncommitted_delta(&w, "b", 6)
		upon_adding_uncommitted_delta(&w, "c", 7)
		upon_clearing_lifecycle_log(&w)

		upon_triggering_bounds_panic(&w, "b", bad_index = 42)

		then_stop_trace_is(&w, "c", "a")
		then_reinit_trace_is(&w, "a", "b", "c")

		if a, ok_a := viewing_worker(&w, "a"); ok_a {
			shows_generation(&w, a, 2)
			shows_value(&w, a, 10)
		}
		if b, ok_b := viewing_worker(&w, "b"); ok_b {
			shows_generation(&w, b, 2)
			shows_value(&w, b, 20)
		}
		if c, ok_c := viewing_worker(&w, "c"); ok_c {
			shows_generation(&w, c, 2)
			shows_value(&w, c, 30)
		}
		then_supervisor_stats_are(&w, "sup", expected_cycles = 1, expected_restarts = 3)
	})
}

@(test)
spec_05_concurrent_sibling_crashes_deduplicate_one_for_all_cycle :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 4)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_All, max_restarts = 3)
		given_supervised_worker(&w, "a", "sup", .Permanent, committed = 10)
		given_supervised_worker(&w, "b", "sup", .Permanent, committed = 20)
		given_supervised_worker(&w, "c", "sup", .Permanent, committed = 30)

		upon_faulting_workers_simultaneously(&w, "a", "b")

		if a, ok_a := viewing_worker(&w, "a"); ok_a {
			shows_generation(&w, a, 2)
		}
		if b, ok_b := viewing_worker(&w, "b"); ok_b {
			shows_generation(&w, b, 2)
		}
		if c, ok_c := viewing_worker(&w, "c"); ok_c {
			shows_generation(&w, c, 2)
		}
		then_supervisor_stats_are(&w, "sup", expected_cycles = 1, expected_restarts = 3)
	})
}

@(test)
spec_06_max_restarts_window_escalation_stops_subtree_and_cascades :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		// Case A: Nested child supervisor escalates past budget to root supervisor.
		given_supervisor(&w, "root", .One_For_One, max_restarts = 3, window = 10 * time.Second)
		given_child_supervisor(&w, "child_sup", under = "root", strategy = .One_For_One, max_restarts = 2, window = 5 * time.Second)
		given_supervised_worker(&w, "w1", "child_sup", .Permanent, committed = 10)
		given_supervised_worker(&w, "w2", "child_sup", .Permanent, committed = 20)

		upon_exiting_worker(&w, "w1", .Panic)
		upon_advancing_clock(&w, 1 * time.Second)
		upon_exiting_worker(&w, "w1", .Panic)
		then_supervisor_escalated(&w, "child_sup", false)

		upon_advancing_clock(&w, 1 * time.Second)
		upon_exiting_worker(&w, "w1", .Panic)
		then_supervisor_escalated(&w, "child_sup", true)
		then_supervisor_stats_are(&w, "root", expected_cycles = 1, expected_restarts = 1)
		then_worker_status_is(&w, "w1", .Running)
		then_worker_status_is(&w, "w2", .Running)

		// Case B: Top-level supervisor stops itself and all children when its own budget blows.
		given_supervisor(&w, "top_sup", .One_For_One, max_restarts = 1, window = 5 * time.Second)
		given_supervised_worker(&w, "leaf", "top_sup", .Permanent, committed = 50)
		upon_exiting_worker(&w, "leaf", .Panic)
		then_supervisor_escalated(&w, "top_sup", false)

		upon_advancing_clock(&w, 1 * time.Second)
		upon_exiting_worker(&w, "leaf", .Panic)
		then_supervisor_escalated(&w, "top_sup", true)
		then_supervisor_is_stopped(&w, "top_sup")
		then_worker_status_is(&w, "leaf", .Stopped)
	})
}

@(test)
spec_07_reinit_failure_consumes_restart_budget_and_recovers :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 5, window = 10 * time.Second)
		given_flaky_reinit_worker(&w, "flaky", "sup", fail_reinit_attempts = 2, committed = 77)
		upon_clearing_lifecycle_log(&w)

		upon_exiting_worker(&w, "flaky", .Panic)

		then_worker_status_is(&w, "flaky", .Running)
		then_supervisor_escalated(&w, "sup", false)
		then_supervisor_stats_are(&w, "sup", expected_cycles = 3, expected_restarts = 3)
		if wk, ok := viewing_worker(&w, "flaky"); ok {
			shows_generation(&w, wk, 4)
			shows_value(&w, wk, 77)
		}
	})
}

@(test)
spec_08_monitor_emits_single_down_with_correct_reason_across_crash_and_stop :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "w_crash", "sup", .Temporary, committed = 1)
		given_supervised_worker(&w, "w_norm", "sup", .Transient, committed = 2)
		given_down_watcher(&w, "watcher_crash")
		given_down_watcher(&w, "watcher_norm")

		_ = upon_monitoring(&w, "watcher_crash", "w_crash")
		_ = upon_monitoring(&w, "watcher_norm", "w_norm")

		upon_exiting_worker(&w, "w_crash", .Panic)
		upon_exiting_worker(&w, "w_crash", .Panic)
		upon_exiting_worker(&w, "w_norm", .Normal)

		then_watcher_received_down(&w, "watcher_crash", count = 1, reason = .Panic)
		then_watcher_received_down(&w, "watcher_norm", count = 1, reason = .Normal)
	})
}

@(test)
spec_09_demonitor_prevents_subsequent_down_delivery :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "w1", "sup", .Temporary, committed = 10)
		given_down_watcher(&w, "watcher")

		m := upon_monitoring(&w, "watcher", "w1")
		upon_demonitoring(&w, m)
		upon_exiting_worker(&w, "w1", .Panic)

		then_worker_status_is(&w, "w1", .Stopped)
		then_watcher_received_down(&w, "watcher", count = 0)
	})
}

@(test)
spec_10_bounded_envelope_pool_conserves_all_slots_under_panic_batch :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 5)
		given_tiny_pool_worker(&w, "w", "sup", slab_capacity = 4, committed = 5)

		upon_sending_4_msg_batch_with_panic_at_3(&w, "w", arena_scratch_bytes = 256)

		wk, ok := viewing_worker(&w, "w")
		if ok {
			shows_generation(&w, wk, 2)
			shows_value(&w, wk, 5)

			// Verify all 4 bounded pool slots were reclaimed by sending a full 4-msg batch.
			batch_ok := [4]Test_Worker_Msg{
				Worker_Add{delta = 3},
				Worker_Add{delta = 3},
				Worker_Add{delta = 3},
				Worker_Add{delta = 3},
			}
			driver_send_worker_batch_and_settle(&w, wk, batch_ok[:])
			shows_value(&w, wk, 17)
		}
	})
}

@(test)
spec_11_sync_try_call_returns_error_on_active_turn_panic :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 3)
		given_supervised_worker(&w, "w", "sup", .Permanent, committed = 0)

		then_sync_call_fails_on_active_panic(&w, "w", .Call_Target_Crashed)
		then_worker_status_is(&w, "w", .Running)
		if wk, ok := viewing_worker(&w, "w"); ok {
			shows_generation(&w, wk, 2)
		}
	})
}

@(test)
spec_12_queued_try_call_fails_on_mailbox_drain_after_preceding_panic :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 3)
		given_supervised_worker(&w, "w", "sup", .Permanent, committed = 0)

		then_queued_sync_call_fails_on_mailbox_drain(&w, "w", .Call_Target_Crashed)
		then_worker_status_is(&w, "w", .Running)
	})
}

@(test)
spec_13_sync_try_call_preserves_completed_reply_if_post_reply_panics :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 3)
		given_supervised_worker(&w, "w", "sup", .Permanent, committed = 0)

		then_sync_call_succeeds_despite_post_reply_panic(&w, "w", reply_val = 4242)
		then_worker_status_is(&w, "w", .Running)
		if wk, ok := viewing_worker(&w, "w"); ok {
			shows_generation(&w, wk, 2)
		}
	})
}

@(test)
spec_14_defer_on_fault_runs_lifo_only_on_fault_not_normal_turn :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 3)
		given_supervised_worker(&w, "w", "sup", .Permanent, committed = 0)

		upon_sending_valid_increments(&w, "w", count = 3, delta = 1)
		then_abort_hooks_ran_in_lifo_order(&w)

		upon_faulting_with_abort_hooks(&w, "w", hook_a = 101, hook_b = 202, arena_scratch_bytes = 512)
		then_abort_hooks_ran_in_lifo_order(&w, 202, 101)
	})
}

@(test)
spec_15_arena_and_temp_allocator_reset_cleanly_across_100_crash_cycles :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 200, window = 60 * time.Second)
		given_supervised_worker(&w, "w", "sup", .Permanent, committed = 42)

		for _ in 0 ..< 100 {
			upon_sending_4_msg_batch_with_panic_at_3(&w, "w", arena_scratch_bytes = 4096)
		}

		then_worker_status_is(&w, "w", .Running)
		if wk, ok := viewing_worker(&w, "w"); ok {
			shows_generation(&w, wk, 101)
			shows_value(&w, wk, 42)
		}
		then_supervisor_stats_are(&w, "sup", expected_cycles = 100, expected_restarts = 100)
	})
}

@(test)
spec_16_single_worker_cooperative_call_isolates_callee_panic :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 1)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 3)
		given_supervised_worker(&w, "callee", "sup", .Permanent, committed = 77)

		then_cooperative_caller_preserves_temp_scratch_on_callee_panic(
			&w,
			callee_label = "callee",
			scratch_tag = "caller-temp-ok",
			expected_err = .Call_Target_Crashed,
		)
		then_worker_status_is(&w, "callee", .Running)
		if callee, ok := viewing_worker(&w, "callee"); ok {
			shows_generation(&w, callee, 2)
			shows_value(&w, callee, 77)
		}
	})
}

@(test)
spec_17_partitioned_supervisor_isolation_and_idle_eviction :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_partitioned_tenant_supervisor(&w, partitions = 4, idle_timeout = 30 * time.Second)

		upon_sending_org_delta(&w, org_id = 101, delta = 10)
		upon_sending_org_delta(&w, org_id = 202, delta = 50)
		then_org_is_registered(&w, 101, true)
		then_org_is_registered(&w, 202, true)
		then_org_value_is(&w, 101, 110)
		then_org_value_is(&w, 202, 150)

		upon_crashing_org_router(&w, org_id = 101, bad_index = 99)
		then_org_value_is(&w, 101, 100)
		then_org_value_is(&w, 202, 150)

		gate := upon_holding_org_turn_open(&w, org_id = 202)
		upon_advancing_clock(&w, 35 * time.Second)
		upon_sweeping_idle_tenants(&w)

		then_evicted_orgs_are(&w, 101)
		then_org_is_registered(&w, 101, false)
		then_org_is_registered(&w, 202, true)
		then_tenant_count_is(&w, 1)

		upon_releasing_org_turn(&w, gate)
		upon_advancing_clock(&w, 35 * time.Second)
		upon_sweeping_idle_tenants(&w)

		then_evicted_orgs_are(&w, 101, 202)
		then_org_is_registered(&w, 202, false)
		then_tenant_count_is(&w, 0)
	})
}

@(test)
prop_18_concurrent_fault_storm_conserves_envelopes_and_converges :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 4)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 500, window = 10 * time.Second)
		labels := [4]string{"s0", "s1", "s2", "s3"}
		for lbl in labels {
			given_tiny_pool_worker(&w, lbl, "sup", slab_capacity = 8, committed = 100)
		}

		for round in 0 ..< 40 {
			for lbl, idx in labels {
				if (round + idx) % 3 == 0 {
					upon_exiting_worker(&w, lbl, .Panic)
				} else {
					upon_adding_uncommitted_delta(&w, lbl, i64(round + 1))
				}
			}
		}

		actor.wait_idle(&w.sched)
		for lbl in labels {
			then_worker_status_is(&w, lbl, .Running)
			val, err := driver_try_read_worker(&w, lbl)
			testing.expect_value(t, err, actor.Call_Error.None)
			testing.expect(t, val >= 100)
		}

		upon_stopping_supervisor(&w, "sup")
		then_no_leaks(&w)
	})
}

// =============================================================================
// Phase 2 & 3 Extended Specifications (`spec_19`..`spec_28`, `prop_29`)
// =============================================================================

@(test)
spec_19_delegated_call_via_forward_and_broken_promise :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "router", "sup", .Permanent, committed = 1)
		given_supervised_worker(&w, "worker", "sup", .Permanent, committed = 2)

		when_trying_forwarded_call(&w, "router", "worker", value = 21, crash_callee = false)
		then_last_call_result_is(&w, expected_val = 42, expected_err = .None)

		when_trying_forwarded_call(&w, "router", "worker", value = 99, crash_callee = true)
		then_last_call_result_is(&w, expected_val = 0, expected_err = .Call_Target_Crashed)

		if router, ok_r := viewing_worker(&w, "router"); ok_r {
			shows_generation(&w, router, 1)
		}
		if wk, ok_w := viewing_worker(&w, "worker"); ok_w {
			shows_generation(&w, wk, 2)
		}
	})
}

@(test)
spec_20_fault_contained_on_stop_hook_during_restart_and_shutdown :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_All, max_restarts = 5)
		given_flaky_stop_worker(&w, "w1", "sup", .Permanent, fail_stop_attempts = 1, fail_stop_mode = .Panic, committed = 10)
		given_flaky_stop_worker(&w, "w2", "sup", .Permanent, fail_stop_attempts = 1, fail_stop_mode = .Panic, committed = 20)
		given_supervised_worker(&w, "w3", "sup", .Permanent, committed = 30)

		upon_exiting_worker(&w, "w1", .Panic)

		then_supervisor_is_running(&w, "sup")
		for lbl, idx in ([3]string{"w1", "w2", "w3"}) {
			then_worker_status_is(&w, lbl, .Running)
			if wk, ok := viewing_worker(&w, lbl); ok {
				shows_generation(&w, wk, 2)
				shows_value(&w, wk, i64((idx + 1) * 10))
			}
		}

		upon_stopping_supervisor(&w, "sup")
		then_supervisor_is_stopped(&w, "sup")
		then_worker_status_is(&w, "w1", .Stopped)
		then_worker_status_is(&w, "w2", .Stopped)
		then_worker_status_is(&w, "w3", .Stopped)
	})
}

@(test)
spec_21_concurrent_shutdown_race_with_self_flood_and_crash_storm :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 4)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 100)
		labels := [4]string{"w0", "w1", "w2", "w3"}
		workers: [4]^Supervised_Test_Worker
		for lbl, idx in labels {
			given_supervised_worker(&w, lbl, "sup", .Permanent, committed = i64(idx))
			workers[idx], _ = viewing_worker(&w, lbl)
		}

		driver_run_shutdown_crash_race(&w, w.supervisors["sup"], workers[:])

		then_supervisor_is_stopped(&w, "sup")
		for lbl in labels {
			then_worker_status_is(&w, lbl, .Stopped)
		}
		then_no_leaks(&w)
	})
}

@(test)
spec_22_temporary_child_cleanup_and_reclamation_under_churn :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		// Part A: `.Temporary` bystander under `.Rest_For_One` is removed after sibling restart.
		given_supervisor(&w, "sup", .Rest_For_One, max_restarts = 5)
		given_supervised_worker(&w, "w1", "sup", .Permanent, committed = 10)
		given_supervised_worker(&w, "w2", "sup", .Temporary, committed = 20)
		given_supervised_worker(&w, "w3", "sup", .Permanent, committed = 30)

		upon_exiting_worker(&w, "w1", .Panic)

		then_worker_status_is(&w, "w1", .Running)
		then_worker_status_is(&w, "w2", .Stopped)
		then_worker_status_is(&w, "w3", .Running)
		then_which_children_excludes(&w, "sup", "w2")
		then_child_counts_are(&w, "sup", actor.Child_Counts{
			specs       = 2,
			active      = 2,
			supervisors = 0,
			workers     = 2,
		})

		// Part B: 500 ephemeral `.Temporary` children are reclaimed with zero table/memory retention.
		given_supervisor(&w, "churn_sup", .One_For_One, max_restarts = 1000)
		churn_sup := w.supervisors["churn_sup"]
		for idx in 0 ..< 500 {
			wk, _, _ := driver_spawn_supervised_worker(&w, churn_sup, "", .Temporary, i64(idx), 0, nil)
			actor.send(&w.sched, wk, Test_Worker_Msg(Worker_Exit{reason = .Panic}))
		}
		actor.wait_idle(&w.sched)
		then_child_counts_are(&w, "churn_sup", actor.Child_Counts{})
	})
}

@(test)
spec_23_child_pid_reuse_and_generation_epoch_guard :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "w", "sup", .Permanent, committed = 5)

		wk, ok := viewing_worker(&w, "w")
		if !ok {
			return
		}
		stale_pid := actor.pid(wk)
		testing.expect_value(t, stale_pid.generation, u32(1))

		upon_exiting_worker(&w, "w", .Panic)
		upon_exiting_worker(&w, "w", .Panic)

		shows_generation(&w, wk, 3)

		// Stale gen-1 PID sends/calls must be rejected without mutating gen-3 worker state.
		actor.send(&w.sched, stale_pid, Test_Worker_Msg(Worker_Add{delta = 50}))
		_, stale_err := actor.try_call(&w.sched, stale_pid, Test_Worker_Msg(Worker_Read{}), i64)
		testing.expect_value(t, stale_err, actor.Call_Error.Stale_Generation)

		fresh_pid := actor.pid(wk)
		testing.expect_value(t, fresh_pid.generation, u32(3))
		val, fresh_err := actor.try_call(&w.sched, fresh_pid, Test_Worker_Msg(Worker_Read{}), i64)
		testing.expect_value(t, fresh_err, actor.Call_Error.None)
		testing.expect_value(t, val, i64(5))
	})
}

@(test)
spec_24_dynamic_child_growth_reverse_stop_and_deep_tree_teardown :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		// Part A: Reverse insertion-order shutdown trace for 5 children.
		given_supervisor(&w, "sup", .One_For_All, max_restarts = 5)
		for lbl, idx in ([5]string{"ch7", "ch3", "ch10", "ch2", "ch5"}) {
			given_supervised_worker(&w, lbl, "sup", .Permanent, committed = i64(idx))
		}
		upon_clearing_lifecycle_log(&w)
		upon_stopping_supervisor(&w, "sup")
		then_stop_trace_is(&w, "ch5", "ch2", "ch10", "ch3", "ch7")

		// Part B: 16-level deep supervisor chain cascades clean shutdown top-to-bottom.
		given_supervisor_chain(&w, "deep_root", depth = 16, leaf_sup_label = "deep_leaf")
		given_supervised_worker(&w, "deep_w", "deep_leaf", .Permanent, committed = 16)
		upon_stopping_supervisor(&w, "deep_root")
		then_supervisor_is_stopped(&w, "deep_root")
		then_supervisor_is_stopped(&w, "deep_leaf")
		then_worker_status_is(&w, "deep_w", .Stopped)

		// Part C: 256-child wide supervisor grows beyond inline cap and shuts down cleanly.
		given_supervisor(&w, "wide_sup", .One_For_One, max_restarts = 10)
		driver_spawn_many_children(&w, w.supervisors["wide_sup"], 256, .Permanent)
		then_child_counts_are(&w, "wide_sup", actor.Child_Counts{
			specs       = 256,
			active      = 256,
			supervisors = 0,
			workers     = 256,
		})
		upon_stopping_supervisor(&w, "wide_sup")
		then_supervisor_is_stopped(&w, "wide_sup")
	})
}

@(test)
spec_25_supervisor_child_admin_and_introspection :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 5, max_children = 3)
		given_supervised_worker(&w, "w1", "sup", .Permanent, committed = 10)
		given_supervised_worker(&w, "w2", "sup", .Permanent, committed = 20)
		given_supervised_worker(&w, "w3", "sup", .Permanent, committed = 30)

		when_spawning_excess_worker(&w, "w4", "sup", .Permanent, committed = 40)
		then_last_supervisor_error_is(&w, .Max_Children_Reached)
		then_child_counts_are(&w, "sup", actor.Child_Counts{specs = 3, active = 3, workers = 3})

		when_deleting_child(&w, "sup", "w2")
		then_last_supervisor_error_is(&w, .Child_Still_Running)

		when_terminating_child(&w, "sup", "w2")
		then_last_supervisor_error_is(&w, .None)
		then_worker_status_is(&w, "w2", .Stopped)
		then_child_counts_are(&w, "sup", actor.Child_Counts{specs = 3, active = 2, workers = 2})

		when_restarting_child(&w, "sup", "w2")
		then_last_supervisor_error_is(&w, .None)
		then_worker_status_is(&w, "w2", .Running)
		if wk, ok := viewing_worker(&w, "w2"); ok {
			shows_generation(&w, wk, 2)
		}
		then_child_counts_are(&w, "sup", actor.Child_Counts{specs = 3, active = 3, workers = 3})

		when_terminating_child(&w, "sup", "w2")
		then_last_supervisor_error_is(&w, .None)
		when_deleting_child(&w, "sup", "w2")
		then_last_supervisor_error_is(&w, .None)
		then_which_children_excludes(&w, "sup", "w2")
		then_child_counts_are(&w, "sup", actor.Child_Counts{specs = 2, active = 2, workers = 2})

		when_spawning_excess_worker(&w, "w4", "sup", .Permanent, committed = 40)
		then_last_supervisor_error_is(&w, .None)
		then_child_counts_are(&w, "sup", actor.Child_Counts{specs = 3, active = 3, workers = 3})
	})
}

@(test)
spec_26_sliding_restart_window_expiry_with_virtual_clock :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 2, window = 100 * time.Millisecond)
		given_supervised_worker(&w, "w", "sup", .Permanent, committed = 50)

		// Spread crashes across sliding 100ms boundaries: expired timestamps must drop off.
		upon_exiting_worker(&w, "w", .Panic) // t=0ms
		when_advancing_clock_ms(&w, 40)
		upon_exiting_worker(&w, "w", .Panic) // t=40ms (window: 2)
		then_worker_status_is(&w, "w", .Running)

		when_advancing_clock_ms(&w, 70) // t=110ms -> t=0ms expired
		upon_exiting_worker(&w, "w", .Panic) // window [40ms, 110ms]: 2
		then_worker_status_is(&w, "w", .Running)

		when_advancing_clock_ms(&w, 50) // t=160ms -> t=40ms expired
		upon_exiting_worker(&w, "w", .Panic) // window [110ms, 160ms]: 2
		then_worker_status_is(&w, "w", .Running)
		then_supervisor_escalated(&w, "sup", false)

		// Rapid burst inside single 100ms window exceeds max_restarts=2.
		when_advancing_clock_ms(&w, 5)
		upon_exiting_worker(&w, "w", .Panic) // window [110ms, 165ms]: 3 > 2 -> escalate
		then_supervisor_escalated(&w, "sup", true)
		then_supervisor_is_stopped(&w, "sup")
	})
}

@(test)
spec_27_registry_duplicate_guard_and_auto_cleanup_on_exit :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t)
		defer world_destroy(&w)

		reg: actor.Registry(u64)
		actor.registry_init(&reg)
		defer actor.registry_destroy(&reg)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "w1", "sup", .Temporary, committed = 11)
		given_supervised_worker(&w, "w2", "sup", .Temporary, committed = 22)
		given_down_watcher(&w, "watcher")

		wk1, ok1 := viewing_worker(&w, "w1")
		wk2, ok2 := viewing_worker(&w, "w2")
		if !ok1 || !ok2 {
			return
		}

		testing.expect(t, actor.registry_register(&reg, 7001, wk1))
		testing.expect(t, !actor.registry_register(&reg, 7001, wk2))

		found_pid, found := actor.registry_lookup(&reg, 7001, Test_Worker_Msg)
		testing.expect(t, found)
		testing.expect(t, found_pid.ref == &wk1.ref)

		m := upon_monitoring(&w, "watcher", "w1")
		defer actor.demonitor(m)

		upon_exiting_worker(&w, "w1", .Panic)
		then_watcher_received_down(&w, "watcher", count = 1, reason = .Panic)

		_, still_registered := actor.registry_lookup(&reg, 7001, Test_Worker_Msg)
		testing.expect(t, !still_registered)
		testing.expect(t, actor.registry_register(&reg, 7001, wk2))
	})
}

@(test)
spec_28_try_call_dead_target_fast_fail_and_multi_call_fanout :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 4)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "dead_w", "sup", .Temporary, committed = 1)
		upon_exiting_worker(&w, "dead_w", .Normal)
		then_worker_status_is(&w, "dead_w", .Stopped)

		if dead_wk, ok := viewing_worker(&w, "dead_w"); ok {
			start_tick := time.tick_now()
			_, dead_err := actor.try_call(&w.sched, dead_wk, Test_Worker_Msg(Worker_Read{}), i64)
			elapsed := time.tick_since(start_tick)
			testing.expect_value(t, dead_err, actor.Call_Error.Target_Stopped)
			testing.expectf(t, elapsed < 5 * time.Millisecond, "dead target call took %v", elapsed)
		}

		fanout: [4]^Supervised_Test_Worker
		for idx in 0 ..< 4 {
			lbl := fmt.tprintf("f%d", idx)
			given_supervised_worker(&w, lbl, "sup", .Permanent, committed = 0)
			fanout[idx], _ = viewing_worker(&w, lbl)
		}

		vals, errs := driver_fanout_try_calls(&w, fanout[:], value = 100, crash_idx = 2, allocator = context.temp_allocator)
		testing.expect_value(t, errs[0], actor.Call_Error.None)
		testing.expect_value(t, vals[0], i64(200))
		testing.expect_value(t, errs[1], actor.Call_Error.None)
		testing.expect_value(t, vals[1], i64(202))
		testing.expect_value(t, errs[2], actor.Call_Error.Call_Target_Crashed)
		testing.expect_value(t, errs[3], actor.Call_Error.None)
		testing.expect_value(t, vals[3], i64(206))
	})
}

@(test)
prop_29_multithreaded_fault_injection_supervision_linearizability :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		for seed in 0 ..< 50 {
			defer free_all(context.temp_allocator)
			ok := driver_run_linearizability_seed(t, u64(seed + 1))
			testing.expectf(t, ok, "linearizability seed %d failed", seed + 1)
			if !ok {
				// One failing seed suffices to confirm Red failure cleanly without wasting suite time.
				break
			}
		}
	})
}

@(test)
spec_30_otp_audit_bugfix_regressions :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		// 1. BUG-009: Dead tenant respawn retains newly registered router in Partitioned_Supervisor registry.
		{
			w: Supervision_World
			world_init(&w, t, num_workers = 2)
			defer world_destroy(&w)

			given_partitioned_tenant_supervisor(&w, partitions = 4, idle_timeout = 30 * time.Second, max_children = 0, policy = .Temporary)
			upon_sending_org_delta(&w, org_id = 303, delta = 10)
			then_org_value_is(&w, org_id = 303, expected = 110)
			then_org_is_registered(&w, org_id = 303, expected = true)

			// Crash temporary tenant router -> tenant becomes dead.
			upon_crashing_org_router(&w, org_id = 303, bad_index = 99)

			// Respawn org 303 -> must remain registered in `ps.cfg.registry`.
			upon_sending_org_delta(&w, org_id = 303, delta = 55)
			then_org_value_is(&w, org_id = 303, expected = 155)
			then_org_is_registered(&w, org_id = 303, expected = true)
		}

		// 2. BUG-029 & BUG-007: Admin terminate + restart of child supervisor and rollback on flaky child reinit.
		{
			w: Supervision_World
			world_init(&w, t, num_workers = 2)
			defer world_destroy(&w)

			given_supervisor(&w, "root", .One_For_One, max_restarts = 3)
			given_child_supervisor(&w, "sub", under = "root", strategy = .One_For_One, max_restarts = 2)
			sub_id := w.next_child_id

			given_supervised_worker(&w, "ok_w", under = "sub", policy = .Permanent, committed = 10)
			given_flaky_reinit_worker(&w, "flaky_w", under = "sub", fail_reinit_attempts = 1, committed = 20)

			root_sup := w.supervisors["root"]
			sub_sup := w.supervisors["sub"]

			// Admin-terminate child supervisor -> sub & grandchildren stopped cleanly.
			testing.expect_value(t, actor.supervisor_terminate_child(root_sup, sub_id), actor.Supervisor_Error.None)
			then_supervisor_is_stopped(&w, "sub")
			then_worker_status_is(&w, "ok_w", .Stopped)
			then_worker_status_is(&w, "flaky_w", .Stopped)

			// First restart attempt hits `flaky_w` reinit panic (BUG-007 rolls back `ok_w` & fails restart cleanly).
			testing.expect_value(t, actor.supervisor_restart_child(root_sup, sub_id), actor.Supervisor_Error.Supervisor_Stopping)
			testing.expect(t, actor.actor_status(sub_sup) != .Running)

			// Second restart attempt succeeds once flaky counter reaches 0 (BUG-029 keeps reset resources).
			testing.expect_value(t, actor.supervisor_restart_child(root_sup, sub_id), actor.Supervisor_Error.None)
			then_supervisor_is_running(&w, "sub")
			then_worker_status_is(&w, "ok_w", .Running)
			then_worker_status_is(&w, "flaky_w", .Running)
		}

		// 3. Multi-worker cooperative `try_call` saturation, caller arena isolation, and cycle detection.
		{
			w: Supervision_World
			world_init(&w, t, num_workers = 4)
			defer world_destroy(&w)

			given_supervisor(&w, "sup", .One_For_One, max_restarts = 5)
			given_supervised_worker(&w, "svc", under = "sup", policy = .Permanent, committed = 100)
			then_multi_worker_cooperative_try_calls_succeed_and_isolate_faults(&w, "svc")
			then_no_leaks(&w)
		}
	})
}
