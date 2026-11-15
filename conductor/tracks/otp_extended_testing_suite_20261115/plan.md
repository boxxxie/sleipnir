# Implementation Plan: OTP ATDD Test Harness, Scenario DSL & Reference Spec Suite (`otp_extended_testing_suite_20261115`)

This plan executes Track 2 in 4 **Red-First 4-Layer ATDD** phases immediately after [`runtime_codegen_hardening_20261115`](../runtime_codegen_hardening_20261115/plan.md) (Track 1) is Green, establishing all Layer 4 contract stubs, Layer 3 protocol drivers, Layer 2 scenario DSLs, and Layer 1 executable Red specifications upfront before [`otp_supervision_20261115`](../otp_supervision_20261115/plan.md) (Track 3) and [`otp_multitenant_and_codegen_20261115`](../otp_multitenant_and_codegen_20261115/plan.md) (Track 4) turn them Green.

---

## Phase 1: Layer 4 Contract Stubs, Layer 3 Driver, Layer 2 DSL & Core OTP Red Specs (`spec_01`–`spec_16`, `prop_18`)

- [x] Task: Create compile-ready Layer 4 API contract stubs (`actor/supervision.odin`, `actor/doc.odin`) and update `ols.json`
  - [x] Define public types, enums, and stubbed procedures in `actor/supervision.odin`:
    - Lifecycle, fault & monitor types: `Actor_Status`, `Fault_Kind`, `Fault_Info`, `Exit_Reason`, `Call_Error` (including `.Target_Stopped`, `.Stale_Generation`, `.Call_Target_Crashed`, `.Deadlock_Detected`, `.Timeout`), `Monitor_Ref`, `Down_Event`, `monitor`, `demonitor`, `exit_turn`, `fail_turn`, `defer_on_fault`, `current_generation`, `Allocator :: mem.Allocator`.
    - `try_call` overloads returning deterministic stub values (`({}, .Target_Stopped)` / non-blocking) so Red specs never hang waiting on unimplemented handlers.
    - Supervisor & admin contracts: `Restart_Strategy`, `Restart_Policy`, `Clock_Proc`, `Supervisor_Config` (including `max_children`), `Supervisor_Msg`, `Supervisor`, `Supervisor_Error`, `Child_Counts`, `Child_Info`, `spawn_supervisor`, `supervise`, `supervise_supervisor`, `supervisor_terminate_child`, `supervisor_restart_child`, `supervisor_delete_child`, `supervisor_count_children`, `supervisor_which_children`.
    - Multi-tenant & registry contracts: `Registry`, `registry_init`, `registry_destroy`, `registry_register`, `registry_unregister`, `registry_lookup`, `default_registry`, `register_name`, `unregister_name`, `whereis_name`, `Partitioned_Supervisor_Config`, `Partitioned_Supervisor`, `Dynamic_Supervisor`, `partitioned_supervisor_init`, `partitioned_supervisor_destroy`, `get_or_spawn_tenant`, `send_to_tenant`, `try_call_tenant`, `call_tenant`, `sweep_idle_tenants`, `tenant_count`, and `dynamic_supervisor_*` wrappers.
  - [x] Export public overloads in `actor/doc.odin` and update `ols.json` `"checker_args"` to `"-custom-attribute:actor,behavior,no_behavior,supervise,reinit"`.

- [x] Task: Build Layer 3 Protocol Driver & Test Probes (`tests/supervision_driver_test.odin`)
  - [x] Implement `Supervision_Driver` wrapping `actor.Scheduler` (supporting `num_workers = 1` cooperative mode and `num_workers >= 2` concurrent execution), `Virtual_Clock` (`virtual_clock_tick` / `advance_ms`), deterministic `settle()`, `Fault_Injector`, `Watcher_Probe` (`monitor` + `Down_Event` capture), and instrumented test actors (`Test_Worker_State`, `Test_Sup_Node`, `init`/`reinit`/`stop` counters).

- [x] Task: Build Layer 2 Scenario DSL (`tests/supervision_dsl_test.odin`)
  - [x] Implement fluent DSL helpers (`loc := #caller_location`) using `testing.expect*`: `given_supervisor`, `given_worker`, `given_sub_supervisor`, `when_sent`, `when_caller_try_calls`, `when_caller_allocates_temp_and_try_calls`, `when_time_advances`, `then_worker_state_is`, `then_restart_order_is`, `then_stop_order_is`, `then_received_down`, `then_down_count_is`, `then_try_call_returns`, `then_supervisor_is_stopped`, `then_supervisor_is_running`, `then_no_leaks`.

- [x] Task: Author Core Layer 1 Red Specifications (`spec_01`–`spec_16`, `prop_18`) in `tests/supervision_spec_test.odin`
  - [x] Write `spec_01` through `spec_16` and `prop_18` inside `tests/supervision_spec_test.odin` wrapped with `with_leak_check`.
  - [x] Verify clean compilation (`odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors`, `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors`) and observe deterministic Red failures on `spec_01`–`spec_16` + `prop_18` while Track 1 tests (`actor_test.odin`, `test_01`–`test_03`) stay Green.

- [x] Task: Conductor - User Manual Verification 'Phase 1: ATDD Harness & Core Red Specs' (Protocol in workflow.md)

---

## Phase 2: Advanced Strategy, Lifecycle Edge Cases & Child Administration Red Specs (`spec_20`–`spec_26`)

- [x] Task: Extend Driver & DSL for lifecycle tracing, cascading faults, shutdown races, deep trees & child admin
  - [x] Extend `tests/supervision_driver_test.odin` and `tests/supervision_dsl_test.odin` with lifecycle trace recorders (`record_stop_order`, `record_reinit_order`), injectable failing `on_stop` / failing `on_reinit` hooks, deep nested supervisor builder (`given_supervisor_chain(depth)`), concurrent crash-vs-shutdown harness, and admin DSL steps (`when_terminating_child`, `when_restarting_child`, `when_deleting_child`, `then_child_counts_are`, `when_advancing_clock_ms`).

- [x] Task: Author `spec_20`–`spec_26` Red Specifications in `tests/supervision_spec_test.odin`
  - [x] Add `spec_20_fault_contained_on_stop_hook_during_restart_and_shutdown`.
  - [x] Add `spec_21_cascading_sibling_on_reinit_failure_rolls_back_restarted_peers`.
  - [x] Add `spec_22_temporary_bystander_stopped_not_restarted_and_specs_reclaimed` (including `500` `.Temporary` child reclamation under `Partitioned_Supervisor`).
  - [x] Add `spec_23_supervisor_shutdown_concurrent_child_crash_race` (`4` workers x `20` iterations, `0` hangs).
  - [x] Add `spec_24_non_alphabetical_child_order_and_deep_tree_iterative_teardown` (`[ch7, ch3, ch10, ch2, ch5]` trace, `16`-level deep chain, `256`-child flat supervisor).
  - [x] Add `spec_25_dynamic_child_administration_and_max_children_backpressure`.
  - [x] Add `spec_26_quiet_period_intensity_reset_survives_multi_burst_faults`.

- [x] Task: Verify Phase 2 Red specs compile cleanly and fail deterministically without hangs or leaks
  - [x] Run `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors`.
  - [x] Execute targeted Red test runs confirming zero hangs and zero test-harness memory leaks.

- [x] Task: Conductor - User Manual Verification 'Phase 2: Advanced Lifecycle & Admin Red Specs' (Protocol in workflow.md)

---

## Phase 3: RPC Call Delegation, Dead-Target Fast-Fail, Multi-Tenant & Registry Red Specs (`spec_17`, `spec_19`, `spec_27`–`spec_28`)

- [x] Task: Extend Driver & DSL for `Partitioned_Supervisor`, `Registry($Key)`, `actor.forward` delegation & multi-call fan-out
  - [x] Extend `tests/supervision_driver_test.odin` and `tests/supervision_dsl_test.odin` with `given_partitioned_supervisor`, `given_tenant_pipeline`, `when_evicting_idle_tenants`, `then_tenant_count_is`, `then_registry_resolves`, router/worker `actor.forward` delegation fixtures, Registry contention probes, and fan-out multi-call batch helpers.

- [x] Task: Author `spec_17`, `spec_19`, `spec_27`, and `spec_28` Red Specifications in `tests/supervision_spec_test.odin`
  - [x] Add `spec_17_partitioned_supervisor_isolation_and_idle_eviction` (multi-tenant crash isolation, `in_flight` call protection against concurrent sweep, two-phase idle TTL eviction via `Virtual_Clock`, `0 B` leaked).
  - [x] Add `spec_19_delegated_call_via_forward_and_broken_promise` (`actor.forward` happy path + broken-promise worker panic wakeup).
  - [x] Add `spec_27_registry_duplicate_guard_and_auto_unenroll_on_permanent_stop` (live-owner duplicate guard, auto-unenrollment on permanent stop, `Down_Event` & monitor edge cleanup).
  - [x] Add `spec_28_try_call_dead_target_fast_fail_and_multi_call_fanout` (dead `.Stopped` / stale-generation `< 1ms` fast-fail + `4`-worker fan-out with partial crash).

- [x] Task: Verify Phase 3 Red specs compile cleanly and fail deterministically without hangs or leaks
  - [x] Run `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors`.
  - [x] Verify `spec_17`, `spec_19`, `spec_27`, `spec_28` fail cleanly on stubs with zero hangs.

- [x] Task: Conductor - User Manual Verification 'Phase 3: Delegation, Multi-Tenant & Registry Red Specs' (Protocol in workflow.md)

---

## Phase 4: Positive & Negative AST Codegen Red Specs (`test_04`–`test_06`) & Multi-Threaded Linearizability Property (`prop_29`)

- [x] Task: Add `Diagnostic` / `validate_module_spec` contract stub in `actor/codegen/codegen.odin`
  - [x] Define `Diagnostic :: struct { file: string, line: int, message: string }` and stub `validate_module_spec(spec: ^Module_Spec, allocator := context.allocator) -> ([]Diagnostic, bool)` in `actor/codegen/codegen.odin`.

- [x] Task: Author Red AST codegen tests (`test_04`–`test_06`) in `tests/codegen_test.odin` and `prop_29` in `tests/supervision_spec_test.odin`
  - [x] Add `test_04_ast_codegen_supervise_and_reinit_attributes` (1-arg `@(reinit)`, 2-arg `@(reinit)` with `actor.Allocator`, exclusion from `<Actor>_Msg`, `<Actor>_PID :: actor.PID(<Actor>_Msg)`, `spawn_*_sup` + reinit trampoline).
  - [x] Add `test_05_codegen_rejects_invalid_supervise_and_reinit_attributes` (invalid strategy string, negative `max_restarts`, duplicate `@(reinit)`, mismatched receiver pointer, mixed `@(behavior)` + `@(reinit)`).
  - [x] Add `test_06_codegen_warns_or_rejects_ambiguous_rpc_reply_signature` (ambiguous/duplicate `reply_to` parameters or malformed reply syntax).
  - [x] Add `prop_29_multithreaded_fault_injection_supervision_linearizability` driving `4` OS worker threads across `50` deterministic PRNG seeds with randomized calls, forwards, monitors, registry updates, child admin commands, panics, and `fail_turn` faults, asserting all 7 invariants (`I1`–`I7`).

- [x] Task: Final Track 2 Gate Verification
  - [x] Run `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors` and `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors`.
  - [x] Confirm Track 1 runtime & codegen tests (`actor_test.odin`, `test_01`–`test_03`) pass (`0` failures, `0 B` leaked) and all Red specs (`spec_01`–`spec_28`, `prop_18`, `prop_29`, `test_04`–`test_06`) fail cleanly without hangs or crashes, ready for Tracks 3 and 4.

- [x] Task: Conductor - User Manual Verification 'Phase 4: Codegen Red Specs & Linearizability Property' (Protocol in workflow.md)
