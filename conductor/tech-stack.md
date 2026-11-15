# Technology Stack

- **Language & Stdlib:** Odin (`base:intrinsics`, `base:runtime`, `core:c/libc`, `core:mem`, `core:mem/virtual`, `core:sync`, `core:time`, `core:thread`)
- **Platform Fault Trapping:** Conditional C FFI bindings for `_setjmp`/`_longjmp` on Linux/FreeBSD/OpenBSD/macOS and `_setjmp(env, nil)` (`RDX = 0` SEH bypass) on Windows x64 (`actor/supervision.odin`) paired with `context.assertion_failure_proc`
- **Code Generation:** `tools/sleipnir_gen` (`actor/codegen/codegen.odin`) using `core:odin/parser` and `core:odin/ast`
- **Testing:** `odin test ./tests` (`actor_test.odin`, `codegen_test.odin`, `supervision_spec_test.odin`) and `odin test examples/05_ast_metaprogramming -custom-attribute:actor,behavior,no_behavior,supervise,reinit` with `mem.Tracking_Allocator`
- **LSP / Collections:** `ols.json` (`sleipnir: .`, `actor: ./actor`, `checker_args: -custom-attribute:actor,behavior,no_behavior,supervise,reinit`)
- **CI:** GitHub Actions (`.github/workflows/ci.yml`)
- **Cloned Reference Repositories (`.tmp/reference_repos/` — gitignored):**
  - Erlang/OTP (`otp`: `lib/stdlib/src/supervisor.erl`, `lib/stdlib/test/supervisor_SUITE.erl`)
  - C++ Actor Framework (`actor-framework`: `libcaf_core/caf/{attachable.hpp,local_actor.cpp,abstract_actor.cpp}`)
  - Elixir (`elixir`: `lib/elixir/lib/{supervisor,dynamic_supervisor,partition_supervisor,registry}.ex` & matching tests)
  - Rust (`ractor` & `ractor-supervisor`: `src/{core.rs,supervisor.rs,dynamic.rs}`)
