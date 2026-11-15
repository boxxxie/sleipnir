# Development Workflow

## Methodology
1. **Red-First 4-Layer ATDD / TDD:**
   - **Layer 1 (`tests/supervision_spec_test.odin`, `tests/actor_test.odin`, `tests/codegen_test.odin`):** Declarative `@(test)` executable specifications (`spec_01`–`spec_28`, `prop_18`, `prop_29`, `test_01`–`test_06`) wrapped in `with_leak_check` + `loc := #caller_location`.
   - **Layer 2 (`tests/supervision_dsl_test.odin`):** Fluent scenario DSL (`given_*`, `when_*`, `then_*`).
   - **Layer 3 (`tests/supervision_driver_test.odin`):** Deterministic cooperative protocol driver (`settle()`, `Virtual_Clock`, `Watcher_Probe`, `Fault_Injector`).
   - **Layer 4 (`actor/*.odin`, `actor/codegen/*.odin`):** Pure runtime and codegen SUT (`package actor`, `package actor_codegen`).
2. **Codegen Sync:** Whenever modifying `actor/codegen/codegen.odin`, regenerate `examples/05_ast_metaprogramming/actors_gen.odin` via `odin run tools/sleipnir_gen -- examples/05_ast_metaprogramming` and verify `git diff --exit-code`.
3. **Mandatory Verification Commands:**
   - `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors`
   - `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors`
   - `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit`
   - `odin test examples/05_ast_metaprogramming -custom-attribute:actor,behavior,no_behavior,supervise,reinit`
   - `odin run examples/01_ping_pong.odin -file -o:speed` (and `examples/library_demo.odin`, `examples/06_otp_supervision.odin`)

## Phase Completion Verification and Checkpointing Protocol
At the end of each phase in a track's `plan.md`:
1. Run `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors`, `odin test ./tests`, and relevant example/codegen checks (both debug and `-o:speed` where applicable), confirming `0` failures and `0` leaked bytes.
2. Check off completed tasks in `plan.md` and review phase changes before advancing to the next phase.
