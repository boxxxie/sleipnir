# Changelog

All notable changes to **Sleipnir** (`package actor`) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Cooperative Shard-Drain Wait in `actor_destroy` & `actor_base_destroy_sched` (`actor/actor.odin`, `actor/supervision.odin`):**
  - Extracted `scheduler_wait_actor_unscheduled` and updated `scheduler_step_one_cooperative` to iterate shards from a rotating offset, skip busy (`in_step != 0`) entries across peer shards, and retire non-runnable (`status != .Running`) shard entries immediately.
  - Prevents a 1-worker (or saturated multi-worker) actor turn from spinning forever in `actor_destroy` / `actor_base_destroy_sched` when destroying a same-scheduler actor whose `scheduled == 1` entry remains queued in `s.shards` after an in-place cooperative `actor.call`.
- **Regression Coverage (`tests/actor_test.odin`):**
  - Added 1-worker `send` + `call` + `actor.destroy` / `actor_base_destroy` shard-drain coverage (with a queued bystander actor) in `test_26_suspended_caller_terminate_runtime_ctx_and_destroy_after_reply`.

---

## [1.1.2] - 2026-11-17

### Fixed
- **Multi-Worker Cooperative `call` / `try_call` Stepping & Isolation (`actor/actor.odin`, `actor/supervision.odin`):**
  - Enabled cooperative target claiming and stepping (`scheduler_run_claimed_actor_cooperative`, `scheduler_try_claim_and_step_cooperative`, and multi-worker `wait_for_call_slot`) so `N` concurrent caller actors on an `N`-worker scheduler cannot park in `sync.futex_wait` while a shared same-scheduler target sits queued with zero awake workers.
  - Added per-thread cooperative call-stack tracking (`_tls_call_stack`, `tls_is_actor_in_step_on_current_thread`, `tls_is_current_actor`, `MAX_COOPERATIVE_CALL_DEPTH`) and `Actor_Base.calling_target` tracking so synchronous call-cycle detection and leaf wait tracing avoid false-positive cycle panics on acyclic DAGs and concurrent fan-in, while deferring `.One_For_All` / `.Rest_For_One` / `terminate_child` teardown until suspended callers unwind.
  - Isolated `_tls_trap_frame`, `context.allocator`, `context.temp_allocator`, and `context.assertion_failure_proc` across cooperative steps, held `in_step` across the Dekker worker tail and post-reply peek with `.Seq_Cst` ordering, woke parked workers on `has_more` reschedules, and enforced `.Seq_Cst` ordering on `finalized` / `stopped_drained` alongside safe stopped and `.Faulted` drains (`actor_drain_stopped_target`, `finalize_actor_stopped`).
- **Regression Coverage (`tests/actor_test.odin`, `tests/supervision_*_test.odin`):**
  - Expanded [`test_24_multi_worker_cooperative_call_saturation_and_cycle_guard`](tests/actor_test.odin) and [`spec_30_otp_audit_bugfix_regressions`](tests/supervision_spec_test.odin) (63 tests in `./tests`) covering 4-worker `N`-to-`1` saturation, acyclic `B -> A -> C` DAGs, cross-worker cycles, arena/temp isolation, deferred `.One_For_All` / `terminate_child` resets, cross-scheduler calls, and `MAX_COOPERATIVE_CALL_DEPTH`.

---

## [1.1.1] - 2026-11-16

### Fixed
- **Step Hot-Path Throughput Recovery (`actor/actor.odin`, `actor/pool.odin`):**
  - Guarded per-batch `free_all(context.temp_allocator)` in `scheduler_worker_proc` so default `temp_allocator` arenas with `total_used == 0` avoid indirect allocator calls on empty-temp batches.
  - Replaced the `1 KB` `Batch_Return_Collector` stack zero-initialization (`memset`) with a `u64` `used_mask` occupancy bitset.
  - Extracted `actor_step_trapped` (`#force_no_inline`) and `step_retire_envelope` out of `actor_generic_step_proc`, hoisted loop-invariant `ctx` fields (`current_actor`, `current_pools_raw`, `current_msg_type`), and cleared `ctx` fields once per batch via `defer`—recovering `Chain=32` fan-in throughput (`29.71 ms` / `67.31 M/s` vs `34.03–34.88 ms`) and A2A scaling.

---

## [1.1.0] - 2026-11-16

### Added
- **OTP Supervision & Fault Tolerance (`actor/supervision.odin`):**
  - `Supervisor` with `.One_For_One`, `.One_For_All`, and `.Rest_For_One` restart strategies, max-restart intensity windows, `.Permanent` / `.Transient` / `.Temporary` child restart policies, and nested supervision subtrees (`supervise_supervisor`).
  - Trapped turn panic recovery (`actor.try_call`, `fail_turn`, `exit_turn`, `defer_on_fault`, `Exit_Reason` / `Fault_Info`), process monitors (`actor.monitor` / `demonitor` / `Down_Event`), and `on_reinit` / `on_stop` lifecycle hooks backed by per-actor `vmem.Arena` reset.
  - `Registry($Key)` typed process registry with automatic cleanup on actor termination or restart.
  - `Partitioned_Supervisor` (and `Dynamic_Supervisor` compatibility wrappers) for sharded multi-tenant actor subtrees with idle tenant eviction and per-tenant `try_call_tenant` / `call_tenant` / `send_to_tenant`.
- **AST Codegen Enhancements (`actor/codegen`, `tools/sleipnir_gen`):**
  - `@(supervise)` and `@(reinit)` attribute support (generating `spawn_<actor>(sup, ...)` overloads and reinit trampolines) plus `@(no_behavior)` filtering and structured AST diagnostics.
- **Examples, Documentation, Tests & CI:**
  - Multi-tenant OTP supervision showcase ([`examples/06_otp_supervision.odin`](examples/06_otp_supervision.odin)).
  - Updated [`README.md`](README.md), [`docs/DSL_GUIDE.md`](docs/DSL_GUIDE.md), and [`tests/README.md`](tests/README.md) with OTP supervision APIs, `@(supervise)` / `@(reinit)` guide, and updated `-custom-attribute` flags.
  - 4-layer ATDD supervision test suite ([`tests/supervision_spec_test.odin`](tests/supervision_spec_test.odin), [`tests/supervision_dsl_test.odin`](tests/supervision_dsl_test.odin), [`tests/supervision_driver_test.odin`](tests/supervision_driver_test.odin); 60 tests in `./tests` plus end-to-end AST DSL test).
  - Parallel GitHub Actions CI workflow ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)), release workflow ([`.github/workflows/release.yml`](.github/workflows/release.yml)), and README badges.

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

[Unreleased]: https://github.com/boxxxie/sleipnir/compare/v1.1.2...HEAD
[1.1.2]: https://github.com/boxxxie/sleipnir/compare/v1.1.1...v1.1.2
[1.1.1]: https://github.com/boxxxie/sleipnir/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/boxxxie/sleipnir/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/boxxxie/sleipnir/releases/tag/v1.0.0
