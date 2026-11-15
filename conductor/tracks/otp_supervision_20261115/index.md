# Track 3: Core OTP Supervision Trees, Per-Actor Panic Recovery, Typed Monitors & `try_call` (`otp_supervision_20261115`)

- [Specification](spec.md)
- [Implementation Plan](plan.md)
- [Metadata](metadata.json)

## Prerequisite & Downstream Tracks
- **Prerequisite Tracks:**
  - [`runtime_codegen_hardening_20261115`](../runtime_codegen_hardening_20261115/index.md) (Track 1 — hardened scheduler, `Call_Slot` offset-0 `Futex`, worker `init_context`, `actor.forward` call-delegation flag preservation)
  - [`otp_extended_testing_suite_20261115`](../otp_extended_testing_suite_20261115/index.md) (Track 2 — pre-authored Layer 1–4 Red ATDD harness & specs `spec_01`–`spec_28`, `prop_18`, `prop_29`, `test_04`–`test_06`)
- **Downstream Track:**
  - [`otp_multitenant_and_codegen_20261115`](../otp_multitenant_and_codegen_20261115/index.md) (Track 4 — turns `spec_17`, `spec_27`, `test_04`–`test_06`, `prop_29` Green)

## Reference Materials (`./references/` — Preserved)
- [OTP Supervision and Panic Recovery in Odin](references/OTP%20Supervision%20and%20Panic%20Recovery%20in%20Odin.md)
- [OTP Carbon Review](references/OTP%20carbon%20review.md)
- [Porting OTP Test Scenarios (Specs 01-18)](references/Porting%20OTP%20test%20scenarios.md)
- Reference Code Snippets: `references/01_current_step_loop.odin` through `references/05_supervision_test.odin`

## Upstream Cloned Reference Repositories (`.tmp/reference_repos/`)
- **Erlang/OTP:** `.tmp/reference_repos/otp/lib/stdlib/src/supervisor.erl` & `lib/stdlib/test/supervisor_SUITE.erl`
- **C++ Actor Framework (CAF):** `.tmp/reference_repos/actor-framework/libcaf_core/caf/{attachable.hpp,local_actor.cpp,abstract_actor.cpp}`
- **Elixir:** `.tmp/reference_repos/elixir/lib/elixir/lib/supervisor.ex` & `lib/elixir/test/elixir/supervisor_test.exs`
- **Rust (`ractor-supervisor`):** `.tmp/reference_repos/ractor-supervisor/src/{core.rs,supervisor.rs}`
