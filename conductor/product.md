# Product Definition: Sleipnir (`package actor`)

## Overview
Sleipnir is a high-throughput, zero-GC, Pony-style actor runtime for [Odin](https://odin-lang.org/). It combines Vyukov intrusive MPSC mailboxes, a sharded work-stealing `M:N` thread scheduler, lock-free `snmalloc`-inspired `Envelope_Pool` slab recycling, wait-free `Sharded_SPSC_Mailbox`, cooperative `actor.call` / `actor.try_call` / `actor.reply`, Erlang/OTP-style supervision trees with per-actor panic recovery, concurrent `Registry($Key)` & sharded multi-tenant `Partitioned_Supervisor`, and `@(actor)` / `@(supervise)` / `@(reinit)` AST metaprogramming (`tools/sleipnir_gen`).

## Target Users
- Systems and backend engineers building concurrent, low-latency, multi-tenant services in Odin.
- Developers needing Erlang/OTP-style fault isolation, supervision trees, typed monitors, and deterministic memory reclamation without a garbage collector.

## Core Capabilities & Goals
1. **Zero-GC Deterministic Memory:** Per-message `Envelope_Pool(Msg)` slab recycling (`pool_id = 0xFFFF` sentinel isolation for heap fallback and dummy stubs) and per-actor `vmem.Arena` + `context.temp_allocator` wiping.
2. **Zero-Ceremony Ergonomics:** Single-line RAII `actor.scoped_scheduler`, envelope-header `actor.call` / `actor.try_call` / `actor.reply`, and `@(actor)` / `@(behavior)` / `@(supervise)` / `@(reinit)` AST codegen with `<Actor>_PID :: actor.PID(<Actor>_Msg)` erasure.
3. **Fault Isolation & OTP Supervision Trees:** Per-actor panic/assertion/bounds-check trapping (`_setjmp`/`_longjmp` + `context.assertion_failure_proc`), `Supervisor` strategies (`.One_For_One`, `.One_For_All`, `.Rest_For_One`) with Erlang/OTP reverse-stop/forward-start ordering, sliding-window restart intensity via injectable `Clock_Proc`, hierarchical supervisor escalation, and `exit_turn`/`fail_turn` + LIFO `defer_on_fault` hooks.
4. **CAF-Style Monitors & Multi-Tenant Supervision:** Typed `monitor`/`demonitor` with safe late-attach `Down_Event` delivery, `Registry($Key)` (`O(1)` lookup under `sync.RW_Mutex`), and sharded `Partitioned_Supervisor` (`Dynamic_Supervisor` compatibility API) with atomic `in_flight` call guards and idle tenant eviction.
