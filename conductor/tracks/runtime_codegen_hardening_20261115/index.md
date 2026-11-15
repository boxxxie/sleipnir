# Track: Baseline Runtime & Codegen Concurrency Hardening (`runtime_codegen_hardening_20261115`)

- [Specification](spec.md)
- [Implementation Plan](plan.md)
- [Metadata](metadata.json)
- [Source Audit: OTP Carbon Review](../otp_supervision_20261115/references/OTP%20carbon%20review.md)

## Overview
This foundational track eliminates every pre-existing concurrency race, memory safety bug, stack use-after-free, worker-context gap, and AST codegen parser defect across `package actor`, `actor/codegen`, `examples/`, `benchmarks/`, and `ols.json` before OTP supervision primitives are built on top of `Actor_Base` and `Scheduler`.

1. **Phase 1:** Mailbox (`actor/mpsc.odin`), Wait-Free SPSC (`actor/spsc.odin`), and Envelope Pool / Watermark Slab (`actor/pool.odin`) hardening.
2. **Phase 2:** Scheduler (`actor/actor.odin`) worker `init_context`, `Call_Slot($Res)` offset-0 `Futex` & UAF elimination, `.Seq_Cst` Dekker unschedule ordering, worker parking, pool contention isolation, cross-scheduler `forward` safety, and `Actor_Base` cache-line lifecycle extension.
3. **Phase 3:** `actor/codegen/codegen.odin` AST parser/emitter fixes, `examples/` & `benchmarks/` `defer` lifetime fixes, `ols.json` custom attributes, and documentation sync.
