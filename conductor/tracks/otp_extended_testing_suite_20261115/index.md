# Track 2: OTP ATDD Test Harness, Scenario DSL & Reference Spec Suite (`otp_extended_testing_suite_20261115`)

- [Specification](spec.md)
- [Implementation Plan](plan.md)
- [Metadata](metadata.json)

## Prerequisite & Downstream Tracks
- **Prerequisite Track:** [`runtime_codegen_hardening_20261115`](../runtime_codegen_hardening_20261115/index.md) (Track 1 — worker `init_context` `Tracking_Allocator` inheritance, `.Seq_Cst` Dekker `wait_idle`, offset-0 `Call_Slot` `Futex`, and `actor.forward` call-delegation flag preservation)
- **Downstream Implementation Tracks (Turn Specs Green):**
  - [`otp_supervision_20261115`](../otp_supervision_20261115/index.md) (Track 3 — turns `spec_01`–`spec_16`, `spec_19`–`spec_26`, `spec_28`, and `prop_18` Green on the first pass)
  - [`otp_multitenant_and_codegen_20261115`](../otp_multitenant_and_codegen_20261115/index.md) (Track 4 — turns `spec_17`, `spec_27`, `test_04`–`test_06`, and `prop_29` Green)

## Upstream Reference Test Suites (`.tmp/reference_repos/`)
- **Erlang/OTP:** `.tmp/reference_repos/otp/lib/stdlib/test/supervisor_SUITE.erl` (`4,052` lines)
- **Elixir:** `.tmp/reference_repos/elixir/lib/elixir/test/elixir/{supervisor,dynamic_supervisor,partition_supervisor,registry}_test.exs` (`1,643` lines)
- **C++ Actor Framework (CAF):** `.tmp/reference_repos/actor-framework/libcaf_core/caf/{actor_termination,typed_response_promise,actor_registry}.test.cpp` (`468` lines)
- **Rust `ractor`:** `.tmp/reference_repos/ractor/ractor/src/{actor/supervision_tests.rs,thread_local/supervision_tests.rs,rpc/tests.rs,registry/tests.rs}` & `.tmp/reference_repos/ractor/ractor/tests/ui/actor_macros/*.rs`
- **Rust `ractor-supervisor`:** `.tmp/reference_repos/ractor-supervisor/src/{supervisor.rs,dynamic.rs}` (`2,049` lines)

## Overview (Red-First 4-Layer ATDD Upfront)
1. **Phase 1: Layer 4 Contract Stubs, Layer 3 Driver, Layer 2 DSL & Core OTP Red Specs (`spec_01`–`spec_16`, `prop_18`)** — Scaffolds compile-ready supervision, monitor, `try_call`, `Registry($Key)`, and `Partitioned_Supervisor` API stubs in `actor/supervision.odin` and `actor/doc.odin`, builds `tests/supervision_driver_test.odin` (`Virtual_Clock`, `Fault_Injector`, `Watcher_Probe`, `settle()`) and `tests/supervision_dsl_test.odin`, and authors the core OTP Red specs.
2. **Phase 2: Advanced Strategy, Lifecycle Edge Cases & Child Administration Red Specs (`spec_20`–`spec_26`)** — Fault-contained `on_stop` hooks, cascading sibling `on_reinit` rollback, `.Temporary` bystander non-restart & memory reclamation, shutdown-vs-crash race, non-alphabetical child ordering, deep supervision tree iterative teardown, `terminate_child`/`restart_child`/`delete_child`/`count_children`/`max_children`, and quiet-period restart counter reset.
3. **Phase 3: RPC Call Delegation, Dead-Target Fast-Fail, Multi-Tenant & Registry Red Specs (`spec_17`, `spec_19`, `spec_27`–`spec_28`)** — `Partitioned_Supervisor` / `Dynamic_Supervisor` multi-tenant isolation & idle eviction (`spec_17`), `actor.forward` call delegation & broken-promise propagation (`spec_19`), Registry duplicate guard + auto-unenrollment on permanent stop + monitor edge cleanup (`spec_27`), and `try_call` fast-fail / fan-out multi-call (`spec_28`).
4. **Phase 4: Positive & Negative AST Codegen Specs (`test_04`–`test_06`) & Multi-Threaded Fault-Injection Linearizability Property (`prop_29`)** — `@(supervise)`/`@(reinit)` positive generation spec (`test_04`), `validate_module_spec` diagnostic stub and negative AST codegen specs (`test_05`–`test_06`), and multi-threaded fault-injection linearizability property (`prop_29`).
