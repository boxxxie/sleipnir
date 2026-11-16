# Changelog

All notable changes to **Sleipnir** (`package actor`) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **OTP Supervision & Fault Tolerance (`actor/supervision.odin`):**
  - `Supervisor` with `.One_For_One`, `.One_For_All`, and `.Rest_For_One` restart strategies, max-restart intensity windows, `.Permanent` / `.Transient` / `.Temporary` child restart policies, and nested supervision subtrees.
  - Panic recovery (`actor.try_call`, trapped turn execution, `Exit_Reason` / `Fault_Info`), process monitors (`actor.monitor` / `DOWN`), and `on_reinit` / `on_stop` lifecycle hooks.
  - `Registry($Key)` typed process registry with automatic cleanup on actor termination.
  - `Partitioned_Supervisor` for sharded multi-tenant actor subtrees with idle tenant eviction and per-tenant `try_call_tenant` / `call_tenant` / `send_to_tenant`.
- **AST Codegen Enhancements (`actor/codegen`, `tools/sleipnir_gen`):**
  - `@(supervise)` and `@(reinit)` attribute support plus `@(no_behavior)` filtering and structured AST diagnostics.
- **Examples, Tests & CI:**
  - Multi-tenant OTP supervision showcase ([`examples/06_otp_supervision.odin`](examples/06_otp_supervision.odin)).
  - 4-layer ATDD supervision test suite ([`tests/supervision_spec_test.odin`](tests/supervision_spec_test.odin), [`tests/supervision_dsl_test.odin`](tests/supervision_dsl_test.odin), [`tests/supervision_driver_test.odin`](tests/supervision_driver_test.odin); 60 total tests).
  - Parallel GitHub Actions CI workflow ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) and README badges.

### Fixed
- Sequential-consistency (`Seq_Cst`) ordering for `base.in_step` loads and `.Release`/`.Acquire` pairing on `last_active_ns` for latest Odin memory-ordering checks.
- Replaced `runtime.args__` with `os.args` in [`benchmarks/actor_benchmark.odin`](benchmarks/actor_benchmark.odin).

---

## [1.0.0] - 2026-11-09

Initial pre-OTP release of **Sleipnir** (`package actor`) at commit [`90d94f5`](https://github.com/boxxxie/sleipnir/commit/90d94f55717e87eba2a88decc95a7657e408f65f).

### Added
- **Lock-Free & Wait-Free Mailboxes (`actor/mpsc.odin`, `actor/spsc.odin`):**
  - Vyukov intrusive MPSC mailbox with `Chain=K` batched splicing (single atomic `XCHG` per slice).
  - Wait-free cache-line-isolated `Sharded_SPSC_Mailbox(T, N)` with zero atomic RMW instructions on the data path (`827M–1.06B msgs/sec`, `0.9–1.2 ns/msg`).
- **Deterministic Zero-GC Memory (`actor/pool.odin`):**
  - `snmalloc`-inspired `Envelope_Pool(Msg)` and `Watermark_Slab` batched envelope recycling integrated with Odin's `context.allocator`.
- **Work-Stealing `M:N` Scheduler (`actor/actor.odin`):**
  - Sharded `RunQ_Shard` work-stealing scheduler with adaptive `sync.Futex` worker parking.
  - Ergonomic pure-Odin runtime DSL: RAII `actor.scoped_scheduler` (`@(deferred_out)`), ambient thread-local context, `PID(Msg)`, compile-time `actor.spawn_self`, envelope-header `actor.call` / `actor.reply`, and zero-copy `iso` `actor.forward`.
- **Optional AST Metaprogramming DSL (`actor/codegen`, `tools/sleipnir_gen`):**
  - Zero-dependency `core:odin/parser` + `core:odin/ast` generator for `@(actor)` and `@(behavior)` annotations, multi-file package scanning, cross-file import forwarding, `->` vtable method calls, and `send_*` / `call_*` proc groups (see [`docs/DSL_GUIDE.md`](docs/DSL_GUIDE.md)).
- **Examples, Tests & Cross-Runtime Benchmarks:**
  - Runnable examples (`examples/01_ping_pong.odin` through `examples/05_ast_metaprogramming/` and `examples/library_demo.odin`).
  - 4-layer ATDD test suite (`tests/actor_test.odin`, `tests/codegen_test.odin`; 22 tests + AST DSL test) with `mem.Tracking_Allocator` zero-leak verification.
  - Cross-runtime benchmark suite (`benchmarks/`) comparing Sleipnir against `libponyrt` `0.77.2`, raw C11 `stdatomic.h` + `pthreads`, `core:sync/chan`, and DOD batch arrays.

### Fixed
- Prevented lost worker wakeups and false-empty MPSC runqueue observations in `scheduler_worker_proc` (`90d94f5`).

[Unreleased]: https://github.com/boxxxie/sleipnir/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/boxxxie/sleipnir/releases/tag/v1.0.0
