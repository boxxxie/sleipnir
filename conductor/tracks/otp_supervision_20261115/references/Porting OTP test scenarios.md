Porting **OTP's test scenarios** alongside **C++ Actor Framework (CAF)** and **Elixir's modern supervision split** will serve you much better than translating Erlang/OTP's source code line-for-line. Most of the subtle bugs we uncovered—stale down signals during cascade restarts, shutdown ordering, and deadlocked `actor.call` waiters—are classic actor-lifecycle edge cases that upstream test suites and typed native runtimes have already mapped out.

**1. Mine `supervisor_SUITE.erl` for Your Layer 1 ATDD Specs (Skip BEAM's C Runtime)**

* Erlang/OTP's `supervisor_SUITE.erl` tests `supervisor.erl` against decades of corner cases, such as children failing synchronously inside `init` (`sup_start_fail`).
* Because Sleipnir already structures `tests/` around Dave Farley's 4-layer ATDD executable specification model (`given_*` / `upon_*` / `then_*` wrapped in `with_leak_check`), translating OTP's test scenarios into Layer 1 Odin specs *before* writing runtime code gives you a behavioral checklist for `OneForOne`, `OneForAll`, and `RestForOne`, `Permanent` / `Transient` / `Temporary` policies, and meltdown windows (`max_restarts` within `max_window`).


* Avoid reading BEAM's C emulator (`erts/`) for design inspiration; Sleipnir has no per-actor garbage collector or cycle detector and recycles fixed-type `Envelope_Pool(Msg)` slabs across an `M:N` work-stealing scheduler.



**2. Reference CAF (`libcaf_core`) for Typed Actors, Monitors, & Request Cancellation**

* Sleipnir's `Actor($State, $Msg)` and `PID($Msg)` mailboxes are statically typed, whereas Erlang's `supervisor.erl` assumes untyped mailboxes that can receive arbitrary `{'EXIT', Pid, Reason}` tuples inline.


* CAF solves this exact mismatch cleanly: it pairs statically typed actor handles with untyped `actor_addr` / `strong_actor_ptr` control addresses for lifecycle tracking—matching Sleipnir's split between type-erased `Actor_Base` and typed `Actor_Ref($Msg)`.


* Rather than polluting user message unions with system messages, CAF attaches out-of-band `set_down_handler` (for unidirectional `monitor`) and `set_exit_handler` (for bidirectional `link_to`) callbacks on the base scheduled actor, routes urgent control traffic separately from normal FIFO messages, and propagates actor exit errors directly back to pending request-response callers—which maps directly to waking Sleipnir's `Call_Slot` (`ENVELOPE_FLAG_CALL`) when a callee dies.



**3. Copy Elixir's `DynamicSupervisor` + `PartitionSupervisor` Split (Not Erlang's `supervisor.erl` Monolith)**

* Erlang's `supervisor.erl` historically bolted dynamic children onto the same module as static trees, whereas Elixir cleanly separated `DynamicSupervisor` into a runtime-only `:one_for_one` manager.
* Better still, Elixir introduced `PartitionSupervisor` specifically so dynamic children can be routed by key (`{name, key}`) across $N$ independent `DynamicSupervisor` processes instead of serializing spawns and lookups through a single bottleneck. In Sleipnir, sharding `Org_<id>` tenants across `num_shards` supervisor actors (`org_id % num_shards`) aligns naturally with your sharded runqueues and eliminates global `RW_Mutex` contention altogether.



**4. Check Rust's `ractor` / `ractor-supervisor` for Non-Selective-Receive State Machines**

* One trap when reading `supervisor.erl` is that Erlang relies on BEAM's selective `receive`—allowing a supervisor to block mid-function waiting for a specific child's shutdown `DOWN` signal while leaving other messages untouched in its mailbox.
* Sleipnir's `MPSC_Queue` is a strict non-selective FIFO queue, and blocking a handler pins a worker thread unless cooperatively stepped. Looking at Rust's `ractor` and `ractor-supervisor` shows how an Erlang-inspired runtime in a non-GC systems language models `SpawnFn` factories, spawn/panic failures, meltdown log resets (`reset_after`), and per-child `backoff_fn` delays cleanly as asynchronous state machines.



Would you like to start by drafting the list of behavioral edge-case specs (adapted from OTP and CAF) for `tests/supervision_test.odin` before touching the runtime implementation?

### 1. Test Suite Layout in `tests/` (`package sleipnir_tests`)

Sleipnir already isolates tests inside `tests/` (`package sleipnir_tests`) to enforce black-box public API boundaries at compile time. Splitting the supervision specifications across three files keeps Layer 1 readable in a 5-second diff while running in parallel under `odin test ./tests -vet -strict-style -disallow-do`:

* **`tests/supervision_spec_test.odin` (Layer 1 — Executable Specifications):** Pure behavioral contracts using string actor labels (`"xitdb"`, `"router"`, `"bot"`), explicit failure/restart inputs, tabular policy matrices, and a seeded fault-storm property test. Contains zero raw pointers, atomics, or allocator plumbing.
* **`tests/supervision_dsl_test.odin` (Layer 2 — Domain DSL):** In-place `Supervision_World` (`world_init(&w, t)` + `defer world_destroy(&w)`), label-to-handle maps, and `given_*` / `upon_*` / `then_*` / `viewing_*` verbs that forward `loc := #caller_location` to every `testing.expect*` call. Zero package globals so concurrent `@(test)` execution never races.
* **`tests/supervision_driver_test.odin` (Layer 3 — Protocol Driver):** Granular helpers invoking `package actor` APIs, recording ordered lifecycle trace events (`Stopped`, `Reinitialized`) into a mutex-guarded log on `Supervision_World`, and verifying zero leaked bytes via `with_leak_check`.



---

### 2. Behavioral Edge-Case Catalog (18 Specs Across 5 Sources)

| # | Spec Name | Upstream Origin | Core Invariant Verified |
| --- | --- | --- | --- |
| **01** | `spec_restart_policy_and_exit_reason_matrix` | OTP `supervisor_SUITE` (`permanent_normal`, `transient_normal`, `temporary`) | Tabular matrix of `{.Permanent, .Transient, .Temporary}` $\times$ `{.Normal, .Shutdown, .Panic_Trapped, .Invalid_State}` verifying exact restart count and terminal status. |
| **02** | `spec_one_for_one_isolates_healthy_siblings` | OTP `supervisor_SUITE` (`one_for_one`) | Crashing one child increments only its generation and resets its state while sibling state, generation, and unread mailbox messages remain untouched. |
| **03** | `spec_rest_for_one_stops_reverse_and_starts_forward` | OTP `supervisor_SUITE` (`rest_for_one`) | When child `1` crashes in `[0, 1, 2]`, child `0` is untouched (`gen == 0`), child `2` stops *before* child `1` resets, and restarts run in forward order `1 -> 2`. |
| **04** | `spec_one_for_all_restarts_entire_cohort_in_order` | OTP `supervisor_SUITE` (`one_for_all`) | When child `1` crashes in `[0, 1, 2]`, surviving children stop in reverse order (`2`, then `0`) and all three reinitialize in forward order (`0 -> 1 -> 2`). |
| **05** | `spec_concurrent_sibling_faults_deduplicate_stale_down` | OTP `supervisor_SUITE` (cascade race) | Simultaneous faults in child `1` and child `2` on separate workers under `.Rest_For_One` drop child `2`'s stale `generation == 0` down signal instead of restarting child `2` twice. |
| **06** | `spec_panic_inside_reinit_counts_toward_intensity` | OTP `supervisor_SUITE` (`sup_start_fail`, `hanging_restart_loop`) | A child whose `@(reinit)` callback panics does not recurse on the supervisor's stack or deadlock; each failed reinit counts against `max_restarts` until escalation. |
| **07** | `spec_high_max_restarts_above_16_escalates_accurately` | Regression guard (`[dynamic; 16]` cap) | A supervisor with `max_restarts = 20` survives 20 faults and escalates on the 21st (preventing silent ring-buffer truncation above 16). |
| **08** | `spec_sliding_window_expires_old_fault_timestamps` | OTP `supervisor_SUITE` (intensity decay) | With `max_restarts = 2` in `50ms`, two faults at `t = 0` followed by a third fault at `t = 80ms` prune expired ticks and restart cleanly without escalating. |
| **09** | `spec_sub_supervisor_escalation_preserves_child_roster` | OTP `supervisor_SUITE` (`restart_escalation`) | When a child supervisor exceeds `max_restarts`, it stops its children and faults up to its parent supervisor, which restarts the child supervisor along with its registered non-`.Temporary` children. |
| **10** | `spec_monitoring_already_stopped_actor_delivers_immediate_down` | CAF `monitor` attachable contract | Calling `monitor` on an actor that has already terminated immediately enqueues a `Down` notification carrying its terminal `Exit_Reason`. |
| **11** | `spec_demonitor_cancels_watch_while_duplicate_monitors_both_fire` | CAF disposable `monitor` tests | Two independent monitors on one worker both receive `Down` on crash, whereas a canceled monitor receives nothing. |
| **12** | `spec_sync_call_returns_error_when_callee_panics_or_drains_mailbox` | CAF request error propagation | `try_call` wakes immediately with `.Call_Target_Crashed` both when the callee panics mid-turn and when a queued `ENVELOPE_FLAG_CALL` message is drained during restart.

 |
| **13** | `spec_panic_after_reply_delivers_result_without_corrupting_caller` | CAF / Sleipnir `Call_Slot` safety

 | If a handler calls `actor.reply(42)` and *then* panics later in the same turn, the caller receives `42` cleanly while the supervisor restarts the callee—zero double-writes to `Call_Slot`.

 |
| **14** | `spec_mid_batch_panic_recycles_all_consumed_envelopes_to_pool` | Sleipnir `Batch_Return_Collector`

 | Across a 4-slot `Envelope_Pool`, crashing on message #3 of a 4-message batch splice flushes messages #1–#3 back to the pool so 32 subsequent sends complete without starvation or leaks.

 |
| **15** | `spec_fault_runs_lifo_abort_hooks_and_wipes_actor_arena` | Sleipnir `vmem.Arena` + `xitdb` tx safety | `defer_on_fault` callbacks run in LIFO order before `vmem.Arena` is wiped in $O(1)$ and `@(reinit)` restores state from the last committed snapshot. |
| **16** | `spec_single_worker_cooperative_call_isolates_callee_panic` | Sleipnir `num_workers = 1` cooperative step

 | On a 1-worker scheduler, a caller actor invoking `try_call` on a panicking callee preserves the caller's `Context` and `context.temp_allocator` scratch data.

 |
| **17** | `spec_partitioned_tenant_supervisor_evicts_idle_and_protects_inflight` | Elixir `PartitionSupervisor` + `DynamicSupervisor` | Sharded `org_id` routing isolates tenant panics, blocks eviction while messages are in flight or scheduled, runs `on_evict` only on idle tenants, and unregisters evicted routes. |
| **18** | `prop_concurrent_fault_storm_conserves_envelopes_and_converges` | Pattern D property invariant | Seeded multi-worker storm of mixed increments, batch sends, `try_call` reads, and bounds-check panics across 8 supervised actors never deadlocks or leaks a single byte.

 |

---

### 3. Draft Layer 1 Specifications (`tests/supervision_spec_test.odin`)

Below is the concrete Layer 1 specification file written against `-vet -strict-style -disallow-do` to lock the behavioral contract before implementing `actor/supervision.odin`:

```odin
package sleipnir_tests

import "base:runtime"
import "core:math/rand"
import "core:testing"
import "core:time"
import "../actor"

// =============================================================================
// Group A: Restart Policies, Strategies & Lifecycle Ordering (OTP Suite)
// =============================================================================

Policy_Case :: struct {
	policy:            actor.Restart_Policy,
	reason:            actor.Exit_Reason,
	expected_restarts: int,
	expected_status:   Actor_Lifecycle_State,
	loc:               runtime.Source_Code_Location,
}

policy_case :: proc(
	policy: actor.Restart_Policy,
	reason: actor.Exit_Reason,
	expected_restarts: int,
	expected_status: Actor_Lifecycle_State,
	loc := #caller_location,
) -> Policy_Case {
	return Policy_Case{
		policy            = policy,
		reason            = reason,
		expected_restarts = expected_restarts,
		expected_status   = expected_status,
		loc               = loc,
	}
}

@(test)
spec_01_restart_policy_and_exit_reason_matrix :: proc(t: ^testing.T) {
	cases := []Policy_Case{
		// Permanent restarts on every exit reason:
		policy_case(.Permanent, .Normal,        1, .Runnable),
		policy_case(.Permanent, .Shutdown,      1, .Runnable),
		policy_case(.Permanent, .Panic_Trapped, 1, .Runnable),
		policy_case(.Permanent, .Invalid_State, 1, .Runnable),

		// Transient restarts only on abnormal exit reasons:
		policy_case(.Transient, .Normal,        0, .Stopped),
		policy_case(.Transient, .Shutdown,      0, .Stopped),
		policy_case(.Transient, .Panic_Trapped, 1, .Runnable),
		policy_case(.Transient, .Invalid_State, 1, .Runnable),

		// Temporary never restarts:
		policy_case(.Temporary, .Normal,        0, .Stopped),
		policy_case(.Temporary, .Panic_Trapped, 0, .Stopped),
		policy_case(.Temporary, .Invalid_State, 0, .Stopped),
	}

	for tc in cases {
		with_leak_check(t, proc(t: ^testing.T) {}) // Sanity check per-test allocator
		w: Supervision_World
		world_init(&w, t, num_workers = 2, loc = tc.loc)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5, loc = tc.loc)
		given_supervised_worker(&w, "worker", under = "sup", policy = tc.policy, committed = 100, loc = tc.loc)

		upon_exiting_worker(&w, "worker", reason = tc.reason, loc = tc.loc)

		then_supervisor_restarts_are(&w, "sup", expected = tc.expected_restarts, loc = tc.loc)
		then_worker_lifecycle_is(&w, "worker", expected = tc.expected_status, loc = tc.loc)

		world_destroy(&w)
	}
}

@(test)
spec_03_rest_for_one_stops_reverse_and_starts_forward :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "org-sup", strategy = .Rest_For_One, max_restarts = 5)
		given_supervised_worker(&w, "xitdb",  under = "org-sup", policy = .Permanent, committed = 10)
		given_supervised_worker(&w, "router", under = "org-sup", policy = .Permanent, committed = 20)
		given_supervised_worker(&w, "bot",    under = "org-sup", policy = .Permanent, committed = 30)

		upon_adding_uncommitted_delta(&w, "xitdb", 5)
		upon_clearing_lifecycle_log(&w)

		// Crashing middle child ("router") must leave "xitdb" untouched while stopping
		// downstream dependents in reverse ("bot", then "router") and restarting forward ("router", then "bot"):
		upon_triggering_bounds_panic(&w, "router", bad_index = 99)

		then_supervisor_restarts_are(&w, "org-sup", expected = 2)
		then_lifecycle_events_are(&w,
			Lifecycle_Event{label = "bot",    kind = .Stopped},
			Lifecycle_Event{label = "router", kind = .Stopped},
			Lifecycle_Event{label = "router", kind = .Reinitialized},
			Lifecycle_Event{label = "bot",    kind = .Reinitialized},
		)

		if db, ok := viewing_worker(&w, "xitdb"); ok {
			shows_generation(&w, db, expected = 0)
			shows_value(&w, db, expected = 15) // Uncommitted +5 survived!
		}
		if router, ok := viewing_worker(&w, "router"); ok {
			shows_generation(&w, router, expected = 1)
			shows_value(&w, router, expected = 20)
		}
		if bot, ok := viewing_worker(&w, "bot"); ok {
			shows_generation(&w, bot, expected = 1)
			shows_value(&w, bot, expected = 30)
		}
	})
}

@(test)
spec_04_one_for_all_restarts_entire_cohort_in_order :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "cohort-sup", strategy = .One_For_All, max_restarts = 5)
		given_supervised_worker(&w, "stage-a", under = "cohort-sup", policy = .Permanent, committed = 1)
		given_supervised_worker(&w, "stage-b", under = "cohort-sup", policy = .Permanent, committed = 2)
		given_supervised_worker(&w, "stage-c", under = "cohort-sup", policy = .Permanent, committed = 3)
		upon_clearing_lifecycle_log(&w)

		upon_triggering_bounds_panic(&w, "stage-b", bad_index = 12)

		then_supervisor_restarts_are(&w, "cohort-sup", expected = 3)
		then_lifecycle_events_are(&w,
			Lifecycle_Event{label = "stage-c", kind = .Stopped},
			Lifecycle_Event{label = "stage-b", kind = .Stopped},
			Lifecycle_Event{label = "stage-a", kind = .Stopped},
			Lifecycle_Event{label = "stage-a", kind = .Reinitialized},
			Lifecycle_Event{label = "stage-b", kind = .Reinitialized},
			Lifecycle_Event{label = "stage-c", kind = .Reinitialized},
		)
	})
}

@(test)
spec_05_concurrent_sibling_faults_deduplicate_stale_down :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 4)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_All, max_restarts = 5)
		given_supervised_worker(&w, "w1", under = "sup", policy = .Permanent, committed = 10)
		given_supervised_worker(&w, "w2", under = "sup", policy = .Permanent, committed = 20)

		// Fault both workers concurrently from generation 0 on barrier release:
		upon_faulting_workers_simultaneously(&w, "w1", "w2")

		// Exactly 1 cohort restart (generation 1 on both), NOT 2 cascades (generation 2):
		if w1, ok := viewing_worker(&w, "w1"); ok {
			shows_generation(&w, w1, expected = 1)
		}
		if w2, ok := viewing_worker(&w, "w2"); ok {
			shows_generation(&w, w2, expected = 1)
		}
	})
}

@(test)
spec_06_panic_inside_reinit_counts_toward_intensity :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 3)
		given_flaky_reinit_worker(&w, "flaky", under = "sup", fail_reinit_attempts = 2, committed = 77)

		// 1st runtime crash + 2 panicking reinit attempts = 3 total restarts; 3rd reinit succeeds:
		upon_triggering_bounds_panic(&w, "flaky", bad_index = 5)

		then_supervisor_restarts_are(&w, "sup", expected = 3)
		then_supervisor_escalated(&w, "sup", expected = false)
		if flaky, ok := viewing_worker(&w, "flaky"); ok {
			shows_value(&w, flaky, expected = 77)
		}
	})
}

// =============================================================================
// Group B: Intensity Windows & Hierarchical Tree Escalation
// =============================================================================

@(test)
spec_07_high_max_restarts_above_16_escalates_accurately :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 20, window = 10 * time.Second)
		given_supervised_worker(&w, "worker", under = "sup", policy = .Permanent, committed = 1)

		for _ in 0 ..< 20 {
			upon_triggering_bounds_panic(&w, "worker", bad_index = 8)
		}
		then_supervisor_escalated(&w, "sup", expected = false)

		// 21st crash within window trips meltdown:
		upon_triggering_bounds_panic(&w, "worker", bad_index = 8)
		then_supervisor_escalated(&w, "sup", expected = true)
		then_worker_lifecycle_is(&w, "worker", expected = .Stopped)
	})
}

@(test)
spec_09_sub_supervisor_escalation_preserves_child_roster :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "root-sup", strategy = .One_For_One, max_restarts = 3)
		given_child_supervisor(&w, "branch-sup", under = "root-sup", strategy = .One_For_All, max_restarts = 1)
		given_supervised_worker(&w, "leaf", under = "branch-sup", policy = .Permanent, committed = 500)

		// 1st crash restarts "leaf" inside "branch-sup"; 2nd crash escalates "branch-sup" to "root-sup",
		// which restarts "branch-sup" AND brings its registered child "leaf" back online:
		upon_triggering_bounds_panic(&w, "leaf", bad_index = 3)
		upon_triggering_bounds_panic(&w, "leaf", bad_index = 3)

		then_supervisor_restarts_are(&w, "root-sup", expected = 1)
		then_worker_lifecycle_is(&w, "leaf", expected = .Runnable)
		if leaf, ok := viewing_worker(&w, "leaf"); ok {
			shows_value(&w, leaf, expected = 500)
		}
	})
}

// =============================================================================
// Group C: Monitors & Synchronous `try_call` Cancellation (CAF Suite)
// =============================================================================

@(test)
spec_10_and_11_monitors_handle_already_stopped_duplicates_and_demonitor :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "temp-worker", under = "sup", policy = .Temporary, committed = 10)
		given_down_watcher(&w, "watcher-active-1")
		given_down_watcher(&w, "watcher-active-2")
		given_down_watcher(&w, "watcher-canceled")
		given_down_watcher(&w, "watcher-late")

		m1 := upon_monitoring(&w, watcher = "watcher-active-1", target = "temp-worker")
		_  = upon_monitoring(&w, watcher = "watcher-active-2", target = "temp-worker")
		m3 := upon_monitoring(&w, watcher = "watcher-canceled", target = "temp-worker")
		_  = m1
		upon_demonitoring(&w, m3)

		upon_exiting_worker(&w, "temp-worker", reason = .Invalid_State)

		then_watcher_received_down(&w, "watcher-active-1", count = 1, reason = .Invalid_State)
		then_watcher_received_down(&w, "watcher-active-2", count = 1, reason = .Invalid_State)
		then_watcher_received_down(&w, "watcher-canceled", count = 0)

		// Monitoring an already-stopped actor fires immediately with its terminal reason:
		_ = upon_monitoring(&w, watcher = "watcher-late", target = "temp-worker")
		then_watcher_received_down(&w, "watcher-late", count = 1, reason = .Invalid_State)
	})
}

@(test)
spec_12_and_13_sync_call_handles_callee_crash_and_post_reply_panic :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "callee", under = "sup", policy = .Permanent, committed = 42)

		// Case A: Callee panics BEFORE calling `actor.reply` -> caller wakes with `.Call_Target_Crashed`
		then_sync_call_fails_on_panic(&w, "callee", expected_err = .Call_Target_Crashed)

		// Case B: Callee calls `actor.reply(42)` and THEN panics -> caller gets `42`, callee still restarts!
		then_sync_call_succeeds_despite_post_reply_panic(&w, "callee", expected_val = 42)
		then_supervisor_restarts_are(&w, "sup", expected = 2)
	})
}

// =============================================================================
// Group D & E: Pool Conservation, Rollback Hooks, Sharded Tenants & Fault Storm
// =============================================================================

@(test)
spec_14_and_15_mid_batch_panic_conserves_4_slot_pool_and_runs_lifo_abort_hooks :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 10)
		given_tiny_pool_worker(&w, "worker", under = "sup", slab_capacity = 4, committed = 100)

		// Registers 2 `defer_on_fault` hooks (`tx_step = 1`, then `tx_step = 2`) and panics on msg #3 of 4:
		upon_sending_4_msg_batch_with_panic_at_3(&w, "worker", arena_scratch_bytes = 16 * 1024)

		then_abort_hooks_ran_in_lifo_order(&w, 2, 1)

		// Sending 32 more messages through the 4-slot non-heap pool proves zero envelopes leaked on panic:
		upon_sending_valid_increments(&w, "worker", count = 32, delta = 1)
		if worker, ok := viewing_worker(&w, "worker"); ok {
			shows_value(&w, worker, expected = 132)
		}
	})
}

@(test)
prop_18_concurrent_fault_storm_conserves_envelopes_and_converges :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		seed :: u64(0x9E3779B97F4A7C15)
		rng_state := rand.create(seed)
		rng := rand.default_random_generator(&rng_state)

		w: Supervision_World
		world_init(&w, t, num_workers = 4)
		defer world_destroy(&w)

		given_supervisor(&w, "storm-sup", strategy = .One_For_One, max_restarts = 500, window = 60 * time.Second)
		labels := [4]string{"node-0", "node-1", "node-2", "node-3"}
		for label in labels {
			given_supervised_worker(&w, label, under = "storm-sup", policy = .Permanent, committed = 1000)
		}

		for iter in 0 ..< 200 {
			defer free_all(context.temp_allocator)
			target := labels[rand.int_max(len(labels), rng)]
			roll := rand.int_max(10, rng)
			if roll < 2 {
				upon_triggering_bounds_panic(&w, target, bad_index = 64 + iter)
			} else if roll < 5 {
				_ = driver_try_read_worker(&w, target) // May return value or `.Call_Target_Crashed` cleanly
			} else {
				upon_adding_uncommitted_delta(&w, target, i64(1 + (iter % 7)))
			}
		}

		// After the fault storm settles, every worker must remain responsive to a final commit + query:
		for label, idx in labels {
			expected_commit := i64(2000 + idx)
			upon_committing_snapshot(&w, label, expected_commit)
			if node, ok := viewing_worker(&w, label); ok {
				shows_value(&w, node, expected = expected_commit)
			}
		}
	})
}

```

Would you like to lock these 18 specs as our target contract and wire up `tests/supervision_dsl_test.odin` + `tests/supervision_driver_test.odin` next (starting Red-First with Group A), or adjust any of the semantics (such as `try_call` error returns vs. caller turn failure)?



To drive implementation Red-First without fighting missing-symbol errors, we wire up four files against Sleipnir's existing `tests/` black-box package (`package sleipnir_tests`):

1. **`actor/supervision.odin` (Layer 4 Contract Skeleton):** Defines the public supervision, monitor, `try_call`, registry, and `Partitioned_Supervisor` types with minimal no-op stubs so `odin check` and `odin test` compile cleanly and fail **behaviorally (Red)** rather than failing to build.
2. **`tests/supervision_spec_test.odin` (Layer 1 — All 18 Executable Specifications):** Complete behavioral contracts (`spec_01`–`spec_17` + `prop_18`) using string labels, row-located policy tables, scoped `viewing_worker` blocks, and stack-seeded property storms.
3. **`tests/supervision_dsl_test.odin` (Layer 2 — Domain DSL):** In-place `Supervision_World` (`world_init(&w, t)` + `defer world_destroy(&w)`), overloaded `proc { ... }` domain verbs, and `loc := #caller_location` attribution on every assertion.
4. **`tests/supervision_driver_test.odin` (Layer 3 — Protocol Driver):** Granular `package actor` adapters, deterministic atomic test clock (`w.clock_ns`—zero flaky `time.sleep` calls), barrier/semaphore coordination for simultaneous faults and mid-turn eviction checks, and leak-free `Envelope_Pool` teardown.



---

### 1. Layer 4 Public API Contract & Red-First Stubs (`actor/supervision.odin`)

```odin
package actor

import "base:runtime"
import "core:mem"
import "core:sync"
import "core:time"

Allocator :: mem.Allocator

Actor_Status :: enum u32 {
	Runnable = 0,
	Faulted  = 1,
	Stopped  = 2,
}

Exit_Reason :: enum u8 {
	Normal = 0,
	Shutdown,
	Panic_Trapped,
	Call_Target_Crashed,
	Invalid_State,
}

Restart_Strategy :: enum u8 {
	One_For_One = 0,
	One_For_All,
	Rest_For_One,
}

Restart_Policy :: enum u8 {
	Permanent = 0,
	Transient,
	Temporary,
}

Call_Error :: enum u8 {
	None = 0,
	Call_Target_Crashed,
	Deadlock_Detected,
	Target_Stopped,
}

Clock_Proc :: #type proc(user_data: rawptr) -> time.Tick

Fault_Info :: struct {
	actor_id:   u32,
	generation: u32,
	reason:     Exit_Reason,
	msg_len:    u8,
	msg_buf:    [128]u8,
	loc:        runtime.Source_Code_Location,
}

Down_Event :: struct {
	monitor_id: u64,
	actor_id:   u32,
	generation: u32,
	reason:     Exit_Reason,
}

Monitor_Ref :: struct {
	id:     u64,
	target: ^Actor_Base,
}

Supervisor_Config :: struct {
	strategy:      Restart_Strategy,
	max_restarts:  int,
	within_window: time.Duration,
	now_proc:      Clock_Proc,
	clock_data:    rawptr,
}

Supervisor_Stats :: struct {
	restart_cycles:     int,
	children_restarted: int,
	escalated:          bool,
	last_fault:         Fault_Info,
}

Child_Hooks :: struct($State: typeid) {
	on_reinit:    proc(state: ^State, arena_alloc: mem.Allocator),
	on_stop:      proc(state: ^State, reason: Exit_Reason),
	use_arena:    bool,
	keep_mailbox: bool,
}

Supervisor_Msg :: struct {
	_placeholder: u8,
}

Supervisor_State :: struct {
	cfg:   Supervisor_Config,
	stats: Supervisor_Stats,
}

Supervisor :: Actor(Supervisor_State, Supervisor_Msg)

@(require_results)
spawn_supervisor :: proc(
	s: ^Scheduler,
	cfg: Supervisor_Config = {},
	id: u32 = 0,
	allocator := context.allocator,
) -> ^Supervisor {
	init_state := Supervisor_State{cfg = cfg}
	sup, _ := spawn(s, init_state, proc(_: ^Supervisor_State, _: Supervisor_Msg) {}, id, allocator)
	return sup
}

supervise :: proc(
	sup: ^Supervisor,
	child: ^Actor($State, $Msg),
	initial_state: State,
	policy: Restart_Policy = .Permanent,
	hooks: Child_Hooks(State) = {use_arena = true, keep_mailbox = false},
) {
	_ = sup
	_ = child
	_ = initial_state
	_ = policy
	_ = hooks
}

supervise_supervisor :: proc(
	parent: ^Supervisor,
	child_sup: ^Supervisor,
	policy: Restart_Policy = .Permanent,
) {
	_ = parent
	_ = child_sup
	_ = policy
}

@(require_results)
supervisor_stats :: proc(sup: ^Supervisor) -> Supervisor_Stats {
	if sup == nil {
		return {}
	}
	return sup.state.stats
}

@(require_results)
actor_status :: proc(a: ^Actor($State, $Msg)) -> Actor_Status {
	_ = a
	return .Runnable
}

@(require_results)
actor_generation :: proc(a: ^Actor($State, $Msg)) -> u32 {
	_ = a
	return 0
}

exit_turn :: proc(
	reason: Exit_Reason = .Normal,
	msg: string = "",
	loc := #caller_location,
) {
	_ = reason
	_ = msg
	_ = loc
}

defer_on_fault :: proc(user_data: rawptr, cleanup: proc(user_data: rawptr)) {
	_ = user_data
	_ = cleanup
}

@(require_results)
monitor :: proc(watcher: PID(Down_Event), target: ^Actor($State, $Msg)) -> Monitor_Ref {
	_ = watcher
	return Monitor_Ref{id = 0, target = &target.base}
}

demonitor :: proc(m: Monitor_Ref) {
	_ = m
}

@(require_results)
try_call_sched_pid :: proc(
	s: ^Scheduler,
	target: PID($Msg),
	msg: Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	_ = s
	_ = target
	_ = msg
	return {}, .Call_Target_Crashed
}

@(require_results)
try_call_sched_actor :: proc(
	s: ^Scheduler,
	target: ^Actor($State, $Msg),
	msg: Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	return try_call_sched_pid(s, pid(target), msg, Res)
}

@(require_results)
try_call_ambient_pid :: proc(
	target: PID($Msg),
	msg: Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	s := resolve_target_scheduler(nil, target.ref)
	return try_call_sched_pid(s, target, msg, Res)
}

@(require_results)
try_call_ambient_actor :: proc(
	target: ^Actor($State, $Msg),
	msg: Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	s := resolve_target_scheduler(nil, &target.ref)
	return try_call_sched_pid(s, pid(target), msg, Res)
}

try_call :: proc {
	try_call_sched_pid,
	try_call_sched_actor,
	try_call_ambient_pid,
	try_call_ambient_actor,
}

// -----------------------------------------------------------------------------
// Named Registry & Partitioned Multi-Tenant Supervisor Contract
// -----------------------------------------------------------------------------

Registry_Entry :: struct {
	ref:      ^Actor_Base,
	msg_type: typeid,
}

Registry :: struct($Key: typeid) {
	mu:        sync.RW_Mutex,
	entries:   map[Key]Registry_Entry,
	allocator: mem.Allocator,
}

registry_init :: proc(r: ^Registry($Key), allocator := context.allocator) {
	r.allocator = allocator
	r.entries   = make(map[Key]Registry_Entry, 16, allocator)
}

registry_destroy :: proc(r: ^Registry($Key)) {
	delete(r.entries)
}

@(require_results)
registry_lookup :: proc(r: ^Registry($Key), key: Key, $Msg: typeid) -> (PID(Msg), bool) {
	sync.rw_mutex_shared_lock(&r.mu)
	entry, found := r.entries[key]
	sync.rw_mutex_shared_unlock(&r.mu)
	if !found || entry.ref == nil || entry.msg_type != typeid_of(Msg) {
		return {}, false
	}
	return PID(Msg){ref = cast(^Actor_Ref(Msg))entry.ref}, true
}

MAX_TENANT_CHILDREN :: 8

Tenant_Subtree :: struct {
	sup:      ^Supervisor,
	router:   ^Actor_Base,
	msg_type: typeid,
	children: [dynamic; MAX_TENANT_CHILDREN]^Actor_Base,
	user_ctx: rawptr,
}

Tenant_Factory_Proc :: #type proc(sched: ^Scheduler, org_id: u64, user_data: rawptr) -> Tenant_Subtree
Tenant_Evict_Proc   :: #type proc(org_id: u64, subtree: ^Tenant_Subtree, user_data: rawptr)

Partitioned_Supervisor_Config :: struct {
	num_partitions: int,
	idle_timeout:   time.Duration,
	factory:        Tenant_Factory_Proc,
	on_evict:       Tenant_Evict_Proc,
	user_data:      rawptr,
	registry:       ^Registry(u64),
	now_proc:       Clock_Proc,
	clock_data:     rawptr,
}

Partitioned_Supervisor :: struct {
	sched:         ^Scheduler,
	cfg:           Partitioned_Supervisor_Config,
	evicted_count: int,
	allocator:     mem.Allocator,
}

partitioned_supervisor_init :: proc(
	ps: ^Partitioned_Supervisor,
	s: ^Scheduler,
	cfg: Partitioned_Supervisor_Config,
	allocator := context.allocator,
) {
	ps.sched         = s
	ps.cfg           = cfg
	ps.evicted_count = 0
	ps.allocator     = allocator
}

partitioned_supervisor_destroy :: proc(ps: ^Partitioned_Supervisor) {
	_ = ps
}

send_to_tenant :: proc(ps: ^Partitioned_Supervisor, org_id: u64, msg: $Msg) {
	_ = ps
	_ = org_id
	_ = msg
}

@(require_results)
try_call_tenant :: proc(
	ps: ^Partitioned_Supervisor,
	org_id: u64,
	msg: $Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	_ = ps
	_ = org_id
	_ = msg
	return {}, .Target_Stopped
}

sweep_idle_tenants :: proc(ps: ^Partitioned_Supervisor) -> int {
	_ = ps
	return 0
}

```

---

### 2. Layer 1: Complete 18 Executable Specifications (`tests/supervision_spec_test.odin`)

```odin
package sleipnir_tests

import "base:runtime"
import "core:math/rand"
import "core:testing"
import "core:time"
import "../actor"

// =============================================================================
// Group A: Restart Policies, Strategies & Lifecycle Ordering (OTP Suite)
// =============================================================================

Policy_Case :: struct {
	policy:            actor.Restart_Policy,
	reason:            actor.Exit_Reason,
	expected_cycles:   int,
	expected_restarts: int,
	expected_status:   actor.Actor_Status,
	loc:               runtime.Source_Code_Location,
}

policy_case :: proc(
	policy: actor.Restart_Policy,
	reason: actor.Exit_Reason,
	expected_cycles: int,
	expected_restarts: int,
	expected_status: actor.Actor_Status,
	loc := #caller_location,
) -> Policy_Case {
	return Policy_Case{
		policy            = policy,
		reason            = reason,
		expected_cycles   = expected_cycles,
		expected_restarts = expected_restarts,
		expected_status   = expected_status,
		loc               = loc,
	}
}

@(test)
spec_01_restart_policy_and_exit_reason_matrix :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		cases := []Policy_Case{
			// Permanent restarts on every exit reason:
			policy_case(.Permanent, .Normal,        1, 1, .Runnable),
			policy_case(.Permanent, .Shutdown,      1, 1, .Runnable),
			policy_case(.Permanent, .Panic_Trapped, 1, 1, .Runnable),
			policy_case(.Permanent, .Invalid_State, 1, 1, .Runnable),

			// Transient restarts only on abnormal exit reasons:
			policy_case(.Transient, .Normal,        0, 0, .Stopped),
			policy_case(.Transient, .Shutdown,      0, 0, .Stopped),
			policy_case(.Transient, .Panic_Trapped, 1, 1, .Runnable),
			policy_case(.Transient, .Invalid_State, 1, 1, .Runnable),

			// Temporary never restarts:
			policy_case(.Temporary, .Normal,        0, 0, .Stopped),
			policy_case(.Temporary, .Panic_Trapped, 0, 0, .Stopped),
			policy_case(.Temporary, .Invalid_State, 0, 0, .Stopped),
		}

		for tc in cases {
			w: Supervision_World
			world_init(&w, t, num_workers = 2, loc = tc.loc)

			given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5, loc = tc.loc)
			given_supervised_worker(&w, "worker", under = "sup", policy = tc.policy, committed = 100, loc = tc.loc)

			upon_exiting_worker(&w, "worker", reason = tc.reason, loc = tc.loc)

			then_supervisor_stats_are(&w, "sup",
				expected_cycles   = tc.expected_cycles,
				expected_restarts = tc.expected_restarts,
				loc               = tc.loc,
			)
			then_worker_status_is(&w, "worker", expected = tc.expected_status, loc = tc.loc)

			world_destroy(&w)
		}
	})
}

@(test)
spec_02_one_for_one_isolates_healthy_siblings :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "healthy",  under = "sup", policy = .Permanent, committed = 10)
		given_supervised_worker(&w, "crashing", under = "sup", policy = .Permanent, committed = 20)

		upon_adding_uncommitted_delta(&w, "healthy",  7)
		upon_adding_uncommitted_delta(&w, "crashing", 9)

		upon_triggering_bounds_panic(&w, "crashing", bad_index = 42)

		then_supervisor_stats_are(&w, "sup", expected_cycles = 1, expected_restarts = 1)
		if healthy, ok := viewing_worker(&w, "healthy"); ok {
			shows_generation(&w, healthy, expected = 0)
			shows_value(&w, healthy, expected = 17)
		}
		if crashing, ok := viewing_worker(&w, "crashing"); ok {
			shows_generation(&w, crashing, expected = 1)
			shows_value(&w, crashing, expected = 20)
		}
	})
}

@(test)
spec_03_rest_for_one_stops_reverse_and_starts_forward :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "org-sup", strategy = .Rest_For_One, max_restarts = 5)
		given_supervised_worker(&w, "xitdb",  under = "org-sup", policy = .Permanent, committed = 10)
		given_supervised_worker(&w, "router", under = "org-sup", policy = .Permanent, committed = 20)
		given_supervised_worker(&w, "bot",    under = "org-sup", policy = .Permanent, committed = 30)

		upon_adding_uncommitted_delta(&w, "xitdb", 5)
		upon_clearing_lifecycle_log(&w)

		upon_triggering_bounds_panic(&w, "router", bad_index = 99)

		then_supervisor_stats_are(&w, "org-sup", expected_cycles = 1, expected_restarts = 2)
		then_lifecycle_events_are(&w,
			Lifecycle_Event{label = "bot",    kind = .Stopped},
			Lifecycle_Event{label = "router", kind = .Stopped},
			Lifecycle_Event{label = "router", kind = .Reinitialized},
			Lifecycle_Event{label = "bot",    kind = .Reinitialized},
		)

		if db, ok := viewing_worker(&w, "xitdb"); ok {
			shows_generation(&w, db, expected = 0)
			shows_value(&w, db, expected = 15)
		}
		if router, ok := viewing_worker(&w, "router"); ok {
			shows_generation(&w, router, expected = 1)
			shows_value(&w, router, expected = 20)
		}
		if bot, ok := viewing_worker(&w, "bot"); ok {
			shows_generation(&w, bot, expected = 1)
			shows_value(&w, bot, expected = 30)
		}
	})
}

@(test)
spec_04_one_for_all_restarts_entire_cohort_in_order :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "cohort-sup", strategy = .One_For_All, max_restarts = 5)
		given_supervised_worker(&w, "stage-a", under = "cohort-sup", policy = .Permanent, committed = 1)
		given_supervised_worker(&w, "stage-b", under = "cohort-sup", policy = .Permanent, committed = 2)
		given_supervised_worker(&w, "stage-c", under = "cohort-sup", policy = .Permanent, committed = 3)

		upon_adding_uncommitted_delta(&w, "stage-a", 10)
		upon_adding_uncommitted_delta(&w, "stage-c", 30)
		upon_clearing_lifecycle_log(&w)

		upon_triggering_bounds_panic(&w, "stage-b", bad_index = 12)

		then_supervisor_stats_are(&w, "cohort-sup", expected_cycles = 1, expected_restarts = 3)
		then_lifecycle_events_are(&w,
			Lifecycle_Event{label = "stage-c", kind = .Stopped},
			Lifecycle_Event{label = "stage-b", kind = .Stopped},
			Lifecycle_Event{label = "stage-a", kind = .Stopped},
			Lifecycle_Event{label = "stage-a", kind = .Reinitialized},
			Lifecycle_Event{label = "stage-b", kind = .Reinitialized},
			Lifecycle_Event{label = "stage-c", kind = .Reinitialized},
		)

		if stage_a, ok := viewing_worker(&w, "stage-a"); ok {
			shows_generation(&w, stage_a, expected = 1)
			shows_value(&w, stage_a, expected = 1)
		}
		if stage_c, ok := viewing_worker(&w, "stage-c"); ok {
			shows_generation(&w, stage_c, expected = 1)
			shows_value(&w, stage_c, expected = 3)
		}
	})
}

@(test)
spec_05_concurrent_sibling_faults_deduplicate_stale_down :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 4)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_All, max_restarts = 5)
		given_supervised_worker(&w, "w1", under = "sup", policy = .Permanent, committed = 10)
		given_supervised_worker(&w, "w2", under = "sup", policy = .Permanent, committed = 20)

		upon_faulting_workers_simultaneously(&w, "w1", "w2")

		then_supervisor_stats_are(&w, "sup", expected_cycles = 1, expected_restarts = 2)
		if w1, ok := viewing_worker(&w, "w1"); ok {
			shows_generation(&w, w1, expected = 1)
		}
		if w2, ok := viewing_worker(&w, "w2"); ok {
			shows_generation(&w, w2, expected = 1)
		}
	})
}

@(test)
spec_06_panic_inside_reinit_counts_toward_intensity :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 3)
		given_flaky_reinit_worker(&w, "flaky", under = "sup", fail_reinit_attempts = 2, committed = 77)

		upon_triggering_bounds_panic(&w, "flaky", bad_index = 5)

		then_supervisor_stats_are(&w, "sup", expected_cycles = 3, expected_restarts = 3)
		then_supervisor_escalated(&w, "sup", expected = false)
		if flaky, ok := viewing_worker(&w, "flaky"); ok {
			shows_value(&w, flaky, expected = 77)
		}
	})
}

// =============================================================================
// Group B: Intensity Windows & Hierarchical Tree Escalation
// =============================================================================

@(test)
spec_07_high_max_restarts_above_16_escalates_accurately :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 20, window = 10 * time.Second)
		given_supervised_worker(&w, "worker", under = "sup", policy = .Permanent, committed = 1)

		for _ in 0 ..< 20 {
			upon_triggering_bounds_panic(&w, "worker", bad_index = 8)
		}
		then_supervisor_escalated(&w, "sup", expected = false)

		upon_triggering_bounds_panic(&w, "worker", bad_index = 8)
		then_supervisor_escalated(&w, "sup", expected = true)
		then_worker_status_is(&w, "worker", expected = .Stopped)
	})
}

@(test)
spec_08_sliding_window_expires_old_fault_timestamps :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 2, window = 50 * time.Millisecond)
		given_supervised_worker(&w, "worker", under = "sup", policy = .Permanent, committed = 50)

		// Exhaust 2 of 2 restarts inside the initial 50ms window:
		upon_triggering_bounds_panic(&w, "worker", bad_index = 4)
		upon_triggering_bounds_panic(&w, "worker", bad_index = 4)
		then_supervisor_escalated(&w, "sup", expected = false)

		// Advance deterministic test clock past the 50ms window so prior ticks expire:
		upon_advancing_clock(&w, 80 * time.Millisecond)

		upon_triggering_bounds_panic(&w, "worker", bad_index = 4)
		upon_triggering_bounds_panic(&w, "worker", bad_index = 4)

		then_supervisor_escalated(&w, "sup", expected = false)
		then_supervisor_stats_are(&w, "sup", expected_cycles = 4, expected_restarts = 4)
		if worker, ok := viewing_worker(&w, "worker"); ok {
			shows_generation(&w, worker, expected = 4)
			shows_value(&w, worker, expected = 50)
		}
	})
}

@(test)
spec_09_sub_supervisor_escalation_preserves_child_roster :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "root-sup", strategy = .One_For_One, max_restarts = 3)
		given_child_supervisor(&w, "branch-sup", under = "root-sup", strategy = .One_For_All, max_restarts = 1)
		given_supervised_worker(&w, "leaf", under = "branch-sup", policy = .Permanent, committed = 500)

		// Crash 1 is absorbed by "branch-sup"; Crash 2 trips "branch-sup" and escalates to "root-sup":
		upon_triggering_bounds_panic(&w, "leaf", bad_index = 3)
		upon_triggering_bounds_panic(&w, "leaf", bad_index = 3)

		then_supervisor_stats_are(&w, "root-sup", expected_cycles = 1, expected_restarts = 1)
		then_supervisor_escalated(&w, "branch-sup", expected = false)
		then_worker_status_is(&w, "leaf", expected = .Runnable)
		if leaf, ok := viewing_worker(&w, "leaf"); ok {
			shows_value(&w, leaf, expected = 500)
		}
	})
}

// =============================================================================
// Group C: Monitors & Synchronous `try_call` Cancellation (CAF Suite)
// =============================================================================

@(test)
spec_10_monitoring_already_stopped_actor_delivers_immediate_down :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "temp", under = "sup", policy = .Temporary, committed = 10)
		given_down_watcher(&w, "late-watcher")

		upon_exiting_worker(&w, "temp", reason = .Invalid_State)
		then_worker_status_is(&w, "temp", expected = .Stopped)

		_ = upon_monitoring(&w, watcher = "late-watcher", target = "temp")
		then_watcher_received_down(&w, "late-watcher", count = 1, reason = .Invalid_State)
	})
}

@(test)
spec_11_demonitor_cancels_watch_while_duplicate_monitors_both_fire :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "temp", under = "sup", policy = .Temporary, committed = 10)
		given_down_watcher(&w, "watcher-dup")
		given_down_watcher(&w, "watcher-single")
		given_down_watcher(&w, "watcher-canceled")

		_  = upon_monitoring(&w, watcher = "watcher-dup",      target = "temp")
		_  = upon_monitoring(&w, watcher = "watcher-dup",      target = "temp")
		_  = upon_monitoring(&w, watcher = "watcher-single",   target = "temp")
		mc := upon_monitoring(&w, watcher = "watcher-canceled", target = "temp")
		upon_demonitoring(&w, mc)

		upon_triggering_bounds_panic(&w, "temp", bad_index = 77)

		then_watcher_received_down(&w, "watcher-dup",      count = 2, reason = .Panic_Trapped)
		then_watcher_received_down(&w, "watcher-single",   count = 1, reason = .Panic_Trapped)
		then_watcher_received_down(&w, "watcher-canceled", count = 0)
	})
}

@(test)
spec_12_sync_call_returns_error_when_callee_panics_or_drains_mailbox :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "callee", under = "sup", policy = .Permanent, committed = 42)

		// 1. Callee panics mid-turn before replying:
		then_sync_call_fails_on_active_panic(&w, "callee", expected_err = .Call_Target_Crashed)

		// 2. Queued `try_call` sitting behind a panicking turn is woken with `.Call_Target_Crashed` on drain:
		then_queued_sync_call_fails_on_mailbox_drain(&w, "callee", expected_err = .Call_Target_Crashed)
	})
}

@(test)
spec_13_panic_after_reply_delivers_result_without_corrupting_caller :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "callee", under = "sup", policy = .Permanent, committed = 42)

		then_sync_call_succeeds_despite_post_reply_panic(&w, "callee", reply_val = 99)
		then_supervisor_stats_are(&w, "sup", expected_cycles = 1, expected_restarts = 1)
		if callee, ok := viewing_worker(&w, "callee"); ok {
			shows_generation(&w, callee, expected = 1)
			shows_value(&w, callee, expected = 42)
		}
	})
}

// =============================================================================
// Group D: Pool Conservation, Rollback Hooks, Single-Worker Call & Tenants
// =============================================================================

@(test)
spec_14_mid_batch_panic_recycles_all_consumed_envelopes_to_pool :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_tiny_pool_worker(&w, "worker", under = "sup", slab_capacity = 4, committed = 100)

		upon_sending_4_msg_batch_with_panic_at_3(&w, "worker", arena_scratch_bytes = 4096)

		// Requires 8x full turnover of the bounded 4-slot non-heap pool:
		upon_sending_valid_increments(&w, "worker", count = 32, delta = 1)
		if worker, ok := viewing_worker(&w, "worker"); ok {
			shows_value(&w, worker, expected = 132)
		}
	})
}

@(test)
spec_15_fault_runs_lifo_abort_hooks_and_wipes_actor_arena :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 2)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "worker", under = "sup", policy = .Permanent, committed = 250)

		upon_faulting_with_abort_hooks(&w, "worker",
			hook_a              = 101,
			hook_b              = 202,
			arena_scratch_bytes = 16 * 1024,
		)

		then_abort_hooks_ran_in_lifo_order(&w, 202, 101)
		if worker, ok := viewing_worker(&w, "worker"); ok {
			shows_generation(&w, worker, expected = 1)
			shows_value(&w, worker, expected = 250)
		}
	})
}

@(test)
spec_16_single_worker_cooperative_call_isolates_callee_panic :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 1)
		defer world_destroy(&w)

		given_supervisor(&w, "sup", strategy = .One_For_One, max_restarts = 5)
		given_supervised_worker(&w, "callee", under = "sup", policy = .Permanent, committed = 10)

		then_cooperative_caller_preserves_temp_scratch_on_callee_panic(&w, "callee",
			scratch_tag  = "req-ctx-77",
			expected_err = .Call_Target_Crashed,
		)
	})
}

@(test)
spec_17_partitioned_tenant_supervisor_evicts_idle_and_protects_inflight :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		w: Supervision_World
		world_init(&w, t, num_workers = 4)
		defer world_destroy(&w)

		given_partitioned_tenant_supervisor(&w, partitions = 4, idle_timeout = 50 * time.Millisecond)

		upon_sending_org_delta(&w, org_id = 101, delta = 40)
		upon_sending_org_delta(&w, org_id = 202, delta = 90)
		gate_303 := upon_holding_org_turn_open(&w, org_id = 303)

		then_org_is_registered(&w, org_id = 101, expected = true)
		then_org_is_registered(&w, org_id = 202, expected = true)
		then_org_is_registered(&w, org_id = 303, expected = true)

		// Crash Org 101; Org 101 rehydrates to base 100 while Org 202 stays at 190:
		upon_crashing_org_router(&w, org_id = 101, bad_index = 88)
		then_org_value_is(&w, org_id = 101, expected = 100)
		then_org_value_is(&w, org_id = 202, expected = 190)

		// Advance clock 100ms, touch Org 202 so it stays fresh, and sweep while Org 303 is mid-turn:
		upon_advancing_clock(&w, 100 * time.Millisecond)
		upon_sending_org_delta(&w, org_id = 202, delta = 10)
		upon_sweeping_idle_tenants(&w)

		// Only idle Org 101 is evicted; active Org 202 and mid-turn Org 303 are protected:
		then_evicted_orgs_are(&w, 101)
		then_org_is_registered(&w, org_id = 101, expected = false)
		then_org_is_registered(&w, org_id = 202, expected = true)
		then_org_is_registered(&w, org_id = 303, expected = true)
		then_org_value_is(&w, org_id = 202, expected = 200)

		// Release Org 303's turn, advance another 100ms, and verify Org 202 & Org 303 evict cleanly:
		upon_releasing_org_turn(&w, gate_303)
		then_org_value_is(&w, org_id = 303, expected = 100)
		upon_advancing_clock(&w, 100 * time.Millisecond)
		upon_sweeping_idle_tenants(&w)
		then_evicted_orgs_are(&w, 101, 202, 303)
	})
}

// =============================================================================
// Group E: Seeded Multi-Worker Fault Storm Property Invariant (Pattern D)
// =============================================================================

@(test)
prop_18_concurrent_fault_storm_conserves_envelopes_and_converges :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		seed :: u64(0x9E3779B97F4A7C15)
		rng_state := rand.create(seed)
		context.random_generator = rand.default_random_generator(&rng_state)

		w: Supervision_World
		world_init(&w, t, num_workers = 4)
		defer world_destroy(&w)

		given_supervisor(&w, "storm-sup", strategy = .One_For_One, max_restarts = 500, window = 60 * time.Second)
		labels := [4]string{"node-0", "node-1", "node-2", "node-3"}
		for label in labels {
			given_supervised_worker(&w, label, under = "storm-sup", policy = .Permanent, committed = 1000)
		}

		for iter in 0 ..< 200 {
			defer free_all(context.temp_allocator)
			target := labels[rand.int_max(len(labels))]
			roll := rand.int_max(10)
			if roll < 2 {
				upon_triggering_bounds_panic(&w, target, bad_index = 64 + iter)
			} else if roll < 5 {
				_, call_err := driver_try_read_worker(&w, target)
				if !testing.expectf(
					t,
					call_err == .None || call_err == .Call_Target_Crashed,
					"seed=%d iter=%d unexpected call_err=%v",
					seed, iter, call_err,
				) {
					return
				}
			} else {
				upon_adding_uncommitted_delta(&w, target, i64(1 + (iter % 7)))
			}
		}

		for label, idx in labels {
			expected_commit := i64(2000 + idx)
			upon_committing_snapshot(&w, label, expected_commit)
			if node, ok := viewing_worker(&w, label); ok {
				shows_value(&w, node, expected = expected_commit)
			}
		}
	})
}

```

---

### 3. Layer 2: Supervision Domain DSL (`tests/supervision_dsl_test.odin`)

```odin
package sleipnir_tests

import vmem "core:mem/virtual"
import "core:slice"
import "core:sync"
import "core:testing"
import "core:time"
import "../actor"

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

Supervision_World :: struct {
	t:              ^testing.T,
	arena:          vmem.Arena,
	sched:          actor.Scheduler,
	clock_ns:       i64,
	supervisors:    map[string]^actor.Supervisor,
	workers:        map[string]^Supervised_Test_Worker,
	watchers:       map[string]^Down_Watcher_Actor,
	tiny_pools:     [dynamic][]actor.Envelope_Pool(Test_Worker_Msg),
	trace_mu:       sync.Mutex,
	lifecycle_log:  [dynamic]Lifecycle_Event,
	abort_hook_log: [dynamic]int,
	evicted_orgs:   [dynamic]u64,
	tenant_reg:     actor.Registry(u64),
	part_sup:       actor.Partitioned_Supervisor,
	has_part_sup:   bool,
}

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

	w.supervisors    = make(map[string]^actor.Supervisor, 8)
	w.workers        = make(map[string]^Supervised_Test_Worker, 16)
	w.watchers       = make(map[string]^Down_Watcher_Actor, 8)
	w.tiny_pools     = make([dynamic][]actor.Envelope_Pool(Test_Worker_Msg), 0, 4)
	w.lifecycle_log  = make([dynamic]Lifecycle_Event, 0, 16)
	w.abort_hook_log = make([dynamic]int, 0, 8)
	w.evicted_orgs   = make([dynamic]u64, 0, 8)
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
	delete(w.lifecycle_log)
	delete(w.watchers)
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
	loc := #caller_location,
) {
	sup := driver_spawn_supervisor(w, strategy, max_restarts, window)
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
	actor.supervise_supervisor(parent, child_sup, .Permanent)
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
	w.workers[label] = driver_spawn_supervised_worker(w, sup, label, policy, committed, 0, nil)
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
	w.workers[label] = driver_spawn_supervised_worker(
		w, sup, label, .Permanent, committed, fail_reinit_attempts, nil,
	)
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
	w.workers[label] = driver_spawn_supervised_worker(w, sup, label, .Permanent, committed, 0, pools)
}

given_down_watcher :: proc(w: ^Supervision_World, label: string) {
	w.watchers[label] = driver_spawn_down_watcher(w)
}

given_partitioned_tenant_supervisor :: proc(
	w: ^Supervision_World,
	partitions: int,
	idle_timeout: time.Duration,
) {
	driver_init_partitioned_tenants(w, partitions, idle_timeout)
}

// -----------------------------------------------------------------------------
// `upon_*` Domain Triggers
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
	driver_send_worker_and_settle(w, worker, Worker_Exit{reason = .Panic_Trapped, bad_index = bad_index})
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
	sync.mutex_unlock(&w.trace_mu)
}

upon_advancing_clock :: proc(w: ^Supervision_World, delta: time.Duration) {
	driver_advance_clock(w, delta)
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
		Worker_Exit{reason = .Panic_Trapped, bad_index = 55, scratch_bytes = arena_scratch_bytes},
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
	actor.send_to_tenant(&w.part_sup, org_id, Test_Worker_Msg(Worker_Commit_Delta{delta = delta}))
	actor.wait_idle(&w.sched)
}

upon_crashing_org_router :: proc(w: ^Supervision_World, org_id: u64, bad_index: int) {
	actor.send_to_tenant(&w.part_sup, org_id, Test_Worker_Msg(Worker_Exit{
		reason    = .Panic_Trapped,
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
	actor.wait_idle(&w.sched)
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
		reason    = .Panic_Trapped,
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

```

---

### 4. Layer 3: Supervision Protocol Driver (`tests/supervision_driver_test.odin`)

```odin
package sleipnir_tests

import "base:intrinsics"
import "core:fmt"
import "core:mem"
import vmem "core:mem/virtual"
import "core:sync"
import "core:thread"
import "core:time"
import "../actor"

// -----------------------------------------------------------------------------
// Worker & Probe Actor Payloads
// -----------------------------------------------------------------------------

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
}

Worker_Slot :: struct {
	world:            ^Supervision_World,
	label:            string,
	committed:        i64,
	fail_reinit_left: int,
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

MAX_WATCHER_EVENTS :: 16

Down_Watcher_State :: struct {
	events: [dynamic; MAX_WATCHER_EVENTS]actor.Down_Event,
}

Down_Watcher_Actor :: actor.Actor(Down_Watcher_State, actor.Down_Event)

// -----------------------------------------------------------------------------
// Deterministic Clock & Lifecycle Hooks
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
record_lifecycle :: proc(w: ^Supervision_World, label: string, kind: Lifecycle_Event_Kind) {
	if w == nil || len(label) == 0 {
		return
	}
	sync.mutex_lock(&w.trace_mu)
	append(&w.lifecycle_log, Lifecycle_Event{label = label, kind = kind})
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
		bounds_probe := [1]int{0}
		_ = bounds_probe[slot.fail_reinit_left + 10]
	}
	state.live_value   = slot.committed
	state.last_scratch = make([]byte, 128, arena_alloc)
	record_lifecycle(slot.world, slot.label, .Reinitialized)
}

@(private = "file")
test_worker_receive :: proc(self: ^Test_Worker_State, msg: Test_Worker_Msg) {
	switch m in msg {
	case Worker_Add:
		self.live_value += m.delta

	case Worker_Commit:
		self.slot.committed = m.new_committed
		self.live_value     = m.new_committed

	case Worker_Commit_Delta:
		self.slot.committed += m.delta
		self.live_value      = self.slot.committed

	case Worker_Read:
		actor.reply(self.live_value)

	case Worker_Exit:
		if m.scratch_bytes > 0 {
			self.last_scratch = make([]byte, m.scratch_bytes)
		}
		self.live_value = -999_999
		if m.reason == .Panic_Trapped {
			arr := [2]int{1, 2}
			_ = arr[m.bad_index]
		} else {
			actor.exit_turn(m.reason, "requested worker exit")
		}

	case Worker_Reply_Then_Panic:
		actor.reply(m.reply_val)
		self.live_value = -999_999
		arr := [2]int{1, 2}
		_ = arr[m.bad_index]

	case Worker_Barrier_Panic:
		sync.barrier_wait(m.barrier)
		arr := [2]int{1, 2}
		_ = arr[99]

	case Worker_Gate_Panic:
		sync.sema_post(&m.gate.entered)
		sync.sema_wait(&m.gate.release)
		if m.do_panic {
			arr := [2]int{1, 2}
			_ = arr[m.bad_index]
		}

	case Worker_Fault_With_Hooks:
		if m.scratch_bytes > 0 {
			self.last_scratch = make([]byte, m.scratch_bytes)
		}
		arena_alloc := vmem.arena_allocator(&self.slot.world.arena)
		ha := new(Abort_Hook_Ctx, arena_alloc)
		hb := new(Abort_Hook_Ctx, arena_alloc)
		ha^ = Abort_Hook_Ctx{world = self.slot.world, step_id = m.hook_a}
		hb^ = Abort_Hook_Ctx{world = self.slot.world, step_id = m.hook_b}
		actor.defer_on_fault(ha, record_abort_hook)
		actor.defer_on_fault(hb, record_abort_hook)

		self.live_value = -999_999
		arr := [2]int{1, 2}
		_ = arr[77]
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
) -> ^actor.Supervisor {
	return actor.spawn_supervisor(&w.sched, actor.Supervisor_Config{
		strategy      = strategy,
		max_restarts  = max_restarts,
		within_window = window,
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
) -> ^Supervised_Test_Worker {
	arena_alloc := vmem.arena_allocator(&w.arena)
	slot := new(Worker_Slot, arena_alloc)
	slot^ = Worker_Slot{
		world            = w,
		label            = label,
		committed        = committed,
		fail_reinit_left = fail_reinit_attempts,
	}

	init_state := Test_Worker_State{
		slot       = slot,
		live_value = committed,
	}

	worker := actor.spawn(&w.sched, init_state, test_worker_receive)
	if explicit_pools != nil {
		worker.pools = explicit_pools
	}

	// Only fail `on_reinit` on subsequent restarts (`fail_reinit_left` armed after initial attach):
	slot.fail_reinit_left = 0
	actor.supervise(sup, worker, init_state, policy, actor.Child_Hooks(Test_Worker_State){
		on_reinit    = test_worker_on_reinit,
		on_stop      = test_worker_on_stop,
		use_arena    = true,
		keep_mailbox = false,
	})
	slot.fail_reinit_left = fail_reinit_attempts
	return worker
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
	})
	caller_thread.data = &tctx
	thread.start(caller_thread)

	for actor.mpsc_is_empty(&worker.mailbox) {
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
			reason    = .Panic_Trapped,
			bad_index = 33,
		}), i64)
		// Clone onto arena so string outlives worker turn temp_allocator reset:
		arena_alloc := vmem.arena_allocator(&req.callee.state.slot.world.arena)
		stable_copy, _ := mem.clone_slice(transmute([]byte)scratch, arena_alloc)
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
// Partitioned Multi-Tenant Driver Wiring (Spec 17)
// -----------------------------------------------------------------------------

driver_init_partitioned_tenants :: proc(
	w: ^Supervision_World,
	partitions: int,
	idle_timeout: time.Duration,
) {
	actor.registry_init(&w.tenant_reg)
	actor.partitioned_supervisor_init(&w.part_sup, &w.sched, actor.Partitioned_Supervisor_Config{
		num_partitions = partitions,
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
				within_window = 10 * time.Second,
				now_proc      = world_clock_proc,
				clock_data    = world,
			})

			init_state := Test_Worker_State{slot = slot, live_value = 100}
			router := actor.spawn(sched, init_state, test_worker_receive, id = u32(org_id))
			actor.supervise(org_sup, router, init_state, .Permanent, actor.Child_Hooks(Test_Worker_State){
				on_reinit    = test_worker_on_reinit,
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
	w.has_part_sup = true
}

@(require_results)
driver_hold_org_turn :: proc(w: ^Supervision_World, org_id: u64) -> ^Tenant_Gate {
	arena_alloc := vmem.arena_allocator(&w.arena)
	gate := new(Tenant_Gate, arena_alloc)
	actor.send_to_tenant(&w.part_sup, org_id, Test_Worker_Msg(Worker_Gate_Panic{
		gate     = gate,
		do_panic = false,
	}))
	sync.sema_wait(&gate.entered)
	return gate
}

```

---

### 5. Red-First Verification Loop

Drop those four files in and run the strict checker + targeted Group A test:

```bash
# 1. Verify strict types, formatting, and zero shadowing across `actor` and `tests`:
odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors
odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors

# 2. Run Spec 01 Red-First (fails on row 1 expecting 1 restart cycle, got 0):
odin test ./tests -vet -strict-style -disallow-do \
  -define:ODIN_TEST_NAMES=sleipnir_tests.spec_01_restart_policy_and_exit_reason_matrix

```

Ready to turn **Group A (`spec_01`–`spec_06`)** and **Group B (`spec_07`–`spec_09`)** Green by implementing the core `Trap_Frame`, `Actor_Base` lifecycle states, and `Supervisor` engine in `actor/actor.odin` + `actor/supervision.odin`?
