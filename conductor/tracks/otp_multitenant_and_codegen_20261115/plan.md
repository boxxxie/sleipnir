# Implementation Plan: Multi-Tenant Partitioned Supervision, `Registry($Key)` & `@(supervise)`/`@(reinit)` Codegen

This plan implements Track 4 in 3 Green-turning phases against the pre-authored Red ATDD specs in [`otp_extended_testing_suite_20261115`](../otp_extended_testing_suite_20261115/plan.md) (Track 2) after [`otp_supervision_20261115`](../otp_supervision_20261115/plan.md) (Track 3) is Green.

---

## Phase 1: Live Reclamation (`actor_base_destroy`), `Registry($Key)` & Sharded `Partitioned_Supervisor` (`spec_17`, `spec_22`, `spec_25`, `spec_27`)

- [x] Task: Implement `actor_base_destroy` (`actor/actor.odin` & `actor/supervision.odin`)
  - [x] Implement `actor_base_destroy(s: ^Scheduler, a: ^Actor_Base)`: mark `.Stopped`, unenroll attached `Registry($Key)` bindings, emit pending `Down_Event` notifications and drain incoming `Monitor_Node` edges, wait until `scheduled == 0 && in_step == 0`, unlink from `s.owned_actors`, drain mailbox (freeing `ENVELOPE_FLAG_HEAP`, returning pool envelopes, failing queued `ENVELOPE_FLAG_CALL` slots with `.Target_Stopped`), destroy `Reset_Desc` (`vmem.arena_destroy`), and call `a.destroy_proc(a)`.

- [x] Task: Implement `Registry($Key)` with duplicate guard & auto-unenroll (`actor/supervision.odin` & `actor/doc.odin`)
  - [x] Implement `Registry($Key)` with `sync.RW_Mutex`, `registry_init`, `registry_destroy`, `registry_register`, `registry_unregister`, `registry_lookup($Key, $Msg)`.
  - [x] Enforce live-owner uniqueness in `registry_register` (returning `false` without overwriting when `key` already maps to a `.Running` actor) and automatically unenroll permanently stopped actors from their registered `Registry($Key)` slots (`spec_27`).
  - [x] Add `default_registry: Registry(u64)` lifecycle initialization/cleanup and export `register_name`, `unregister_name`, `whereis_name` in `actor/doc.odin` with zero leaks in `with_leak_check`.

- [x] Task: Implement sharded `Partitioned_Supervisor` & `Dynamic_Supervisor` compatibility API (`actor/supervision.odin`)
  - [x] Implement `Partitioned_Supervisor` (`DEFAULT_PARTITIONS :: 64`, shard index `org_id % u64(len(partitions))`) with per-partition `sync.RW_Mutex`, `tenants: map[u64]^Tenant_Entry`, and `max_children` capacity enforcement (`.Max_Children_Reached`, `spec_25`).
  - [x] Implement atomic `in_flight` reference counting around `get_or_spawn_tenant`, `send_to_tenant`, `try_call_tenant`, and `call_tenant`.
  - [x] Ensure `.Temporary` workers exiting or crashing under `Partitioned_Supervisor` / `Dynamic_Supervisor` immediately reclaim `Tenant_Entry` / `Child_Spec` / `Reset_Desc` allocations via `actor_base_destroy` (`spec_22`).
  - [x] Implement two-phase `sweep_idle_tenants`: Phase 1 collects idle `in_flight == 0` entries under `part.mu` and removes keys after loop exit; Phase 2 destroys evicted tenant supervisor/worker actors via `actor_base_destroy` outside the map scan (`spec_17`).
  - [x] Expose `Dynamic_Supervisor` alias and `dynamic_supervisor_*` wrappers backed by `Partitioned_Supervisor`.

- [x] Task: Verify Phase 1 turns `spec_17`, `spec_22`, `spec_25`, and `spec_27` Green with zero leaks
  - [x] Run `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors`.
  - [x] Run `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit` for `spec_17`, `spec_22`, `spec_25`, `spec_27`.

- [x] Task: Conductor - User Manual Verification 'Phase 1: Partitioned_Supervisor, Registry & Reclamation' (Protocol in workflow.md)

---

## Phase 2: `@(supervise)` & `@(reinit)` AST Codegen + `validate_module_spec` Negative Diagnostics (`test_04`, `test_05`, `test_06`)

- [x] Task: Implement `@(supervise)` and `@(reinit)` AST parsing and emission (`actor/codegen/codegen.odin`, `actor/doc.odin`)
  - [x] Confirm `Allocator :: mem.Allocator` alias in `actor/doc.odin`.
  - [x] Extend `Actor_Spec` in `actor/codegen/codegen.odin` with `Reinit_Spec` and `Supervise_Spec` (`strategy`, `restart`, `max_restarts`, `window_ms`, `use_arena`).
  - [x] Update `parse_source_into_spec` and `finalize_module_spec` to collect `pending_reinits` and attach them to their target `Actor_Spec` while skipping them as message behaviors.
  - [x] Emit `_<snake>_reinit_trampoline`, `<Actor>_DEFAULT_RESTART_POLICY`, and `spawn_<snake>_sup` overloads in `emit_module_code` (`test_04`).

- [x] Task: Implement `validate_module_spec` diagnostics (`actor/codegen/codegen.odin` & `tools/sleipnir_gen/main.odin`)
  - [x] Implement structured `Diagnostic` reporting in `validate_module_spec(spec: ^Module_Spec, allocator := context.allocator) -> ([]Diagnostic, bool)` rejecting invalid `@(supervise)` options, duplicate/mismatched `@(reinit)` procs, mixed `@(behavior)` + `@(reinit)`, and ambiguous/malformed `reply_to` signatures (`test_05`, `test_06`).
  - [x] Wire diagnostic printing and non-zero exit code into `tools/sleipnir_gen/main.odin` on invalid input.

- [x] Task: Verify Phase 2 turns `test_04`, `test_05`, and `test_06` Green
  - [x] Run `odin run tools/sleipnir_gen -- examples/05_ast_metaprogramming`.
  - [x] Run `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit`.
  - [x] Run `odin test examples/05_ast_metaprogramming -custom-attribute:actor,behavior,no_behavior,supervise,reinit`.

- [x] Task: Conductor - User Manual Verification 'Phase 2: @(supervise)/@(reinit) Codegen & Diagnostics' (Protocol in workflow.md)

---

## Phase 3: Multi-Threaded Linearizability (`prop_29`), Showcase (`examples/06_otp_supervision.odin`) & CI Pipeline Verification

- [x] Task: Stabilize and verify `prop_29_multithreaded_fault_injection_supervision_linearizability`
  - [x] Run `prop_29` across `4` OS worker threads and `50` deterministic PRNG seeds in both debug and `-o:speed` builds, confirming all 7 invariants (`I1`–`I7`) pass with `0 B` leaked and zero flaky sleeps.

- [x] Task: Author `examples/06_otp_supervision.odin` end-to-end showcase
  - [x] Implement 50-org x 3-stage pipeline (`Validator -> Enricher -> Writer`) under per-tenant `.Rest_For_One` `Supervisor` instances managed by `Partitioned_Supervisor` / `Dynamic_Supervisor`.
  - [x] Use `actor.current_generation()` and thread-safe `try_call_tenant` / `send_to_tenant` queries (no unsafe worker-memory dereferences from `main`).
  - [x] Demonstrate injected pipeline panics, `.Rest_For_One` reverse-stop/forward-reinit recovery, `try_call` `.Call_Target_Crashed` handling, idle tenant eviction via `sweep_idle_tenants`, and `mem.Tracking_Allocator` verifying `0` leaked bytes.

- [x] Task: Verify `ols.json` and update `.github/workflows/ci.yml`
  - [x] Confirm `ols.json` `"checker_args"` is `"-custom-attribute:actor,behavior,no_behavior,supervise,reinit"`.
  - [x] Update `.github/workflows/ci.yml` to run full strict checks, `odin test ./tests` (debug + `-o:speed`), codegen diff check, `examples/05_ast_metaprogramming`, and `examples/06_otp_supervision.odin` (`-file` and `-file -o:speed`).

- [x] Task: Final Full-Repository Verification (`spec_01`–`spec_28`, `prop_18`, `prop_29`, `test_01`–`test_06`)
  - [x] Run every CI step locally (`odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors`, `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors`, `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit`, `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit -o:speed`, `odin test examples/05_ast_metaprogramming -custom-attribute:actor,behavior,no_behavior,supervise,reinit`, `odin run examples/06_otp_supervision.odin -file -o:speed`, and `odin doc ./actor > /dev/null`).

- [x] Task: Conductor - User Manual Verification 'Phase 3: Linearizability, Multi-Tenant Demo & CI Verification' (Protocol in workflow.md)
