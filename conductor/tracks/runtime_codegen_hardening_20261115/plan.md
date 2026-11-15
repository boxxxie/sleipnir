# Implementation Plan: Baseline Runtime & Codegen Concurrency Hardening

This plan follows Test-Driven Development (TDD) and Sleipnir's 4-layer testing architecture. Each phase adds failing regression/concurrency tests first (**Red**), fixes the underlying runtime or codegen code (**Green**), and verifies zero leaks and clean static analysis (**Refactor & Verify**).

---

## Phase 1: Mailbox (`mpsc.odin`), Wait-Free SPSC (`spsc.odin`), and Pool (`pool.odin`) Hardening

- [x] Task: Write failing unit/regression tests for MPSC/SPSC/Pool invariants
  - [x] Write failing test in `tests/actor_test.odin` verifying `spsc_consume_batch` returns `0` cleanly when `max_batch <= 0` without reading out-of-bounds or wrapping `u64`.
  - [x] Write failing test verifying `pool_acquire` and `watermark_slab_acquire` / `watermark_slab_acquire_chain` reset `reply_slot == nil` and `flags == ENVELOPE_FLAG_NONE` on recycled envelopes.
  - [x] Write failing test verifying `mpsc_init` sets `q.stub.pool_id == 0xFFFF` and `pool_init` with `slab_capacity <= 0` does not hang on `pool_acquire`.

- [x] Task: Fix atomic `q.tail` stores and `stub.pool_id` sentinel in `actor/mpsc.odin`
  - [x] Update `mpsc_init` (`actor/mpsc.odin:29-33`) to initialize `q.stub.pool_id = 0xFFFF` and `q.stub.flags = ENVELOPE_FLAG_STATIC` (using appropriate `when intrinsics.type_has_field(T, ...)` compile-time guards so `MPSC_Queue` remains generic for `RunQ_Shard(Actor_Base)`).
  - [x] Replace plain `q.tail = next` assignments in `mpsc_pop_detached` (`lines 76, 82, 102`) and `mpsc_pop_dummy` (`line 116`) with `intrinsics.atomic_store_explicit(&q.tail, next, .Relaxed)`.
  - [x] Document the non-movable interior-pointer invariant on `MPSC_Queue`.

- [x] Task: Fix `spsc_consume_batch` and `Envelope_Pool` / `Watermark_Slab` safety in `actor/spsc.odin` and `actor/pool.odin`
  - [x] Add `if max_batch <= 0 { return 0 }` guard at entry of `spsc_consume_batch` (`actor/spsc.odin:77`).
  - [x] Zero `env.reply_slot = nil` and `env.flags = ENVELOPE_FLAG_NONE` in all reuse/bump/reclaim branches of `pool_acquire` (`actor/pool.odin:93-135`), `watermark_slab_acquire` (`line 267-270`), and `watermark_slab_acquire_chain` (`line 289-303`).
  - [x] Guard `watermark_slab_acquire_chain` against `k <= 0 || k >= N - 2` (`assert(k > 0 && k < N - 2)` / early return) so dummy-tail retirement can never underflow or spin forever.
  - [x] Update `pool_init` (`actor/pool.odin:59-77`) so `slab_capacity <= 0` forces `p.heap_fallback = true` instead of spinning forever on first acquire.

- [x] Task: Verify Phase 1 tests pass with zero memory leaks
  - [x] Run `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors`.
  - [x] Run `odin test ./tests` and confirm all Phase 1 tests pass with `0` leaks.

- [x] Task: Conductor - User Manual Verification 'Phase 1: Mailbox, SPSC & Pool Hardening' (Protocol in workflow.md)

---

## Phase 2: Scheduler Worker Context, Dekker Barrier, `Call_Slot` & `Actor_Base` Hardening (`actor/actor.odin`)

- [x] Task: Write failing concurrency and safety tests in `tests/actor_test.odin`
  - [x] Write failing test verifying worker handlers inherit caller `context.allocator` (`Tracking_Allocator` detects intentional temporary allocations cleanly or frees match) and wipe `context.temp_allocator` per top-level turn without wiping caller scratch across nested `actor.call`.
  - [x] Write failing stress test for `actor.call` / `actor.reply` verifying: no stack UAF across thousands of cross-thread calls, duplicate `actor.reply` calls are ignored safely, and `1`-worker self-call / `A -> B -> A` cycle returns/errors safely instead of hanging or blowing the stack.
  - [x] Write failing stress test verifying Dekker unschedule (`wait_idle` never returns while envelopes remain), heap-fallback envelopes (`pool_id = 0xFFFF`, `ENVELOPE_FLAG_HEAP`) do not corrupt `pools[0]` or leak on `actor.destroy`, and cross-scheduler `send` / `forward` works cleanly without pool corruption.

- [x] Task: Extend `Actor_Base` and harden `Scheduler` lifecycle (`actor/actor.odin`)
  - [x] Extend `Actor_Base` (`actor/actor.odin:68-76`) with `status: u32`, `in_step: u32`, and `generation: u32` inside Cache Line 0 tail bytes so `size_of(Actor_Base) == 64` or clean multi-line alignment is preserved for Tracks 2–4.
  - [x] Fix `scheduler_init` (`actor/actor.odin:179-210`) to respect `0 < cfg.num_shards < s.num_workers`, add partial-allocation rollback on error, and avoid clobbering TLS `_tls_active_sched` unexpectedly or restore on destroy.
  - [x] Update `scheduler_start` (`actor/actor.odin:288-291`) to set `sched.workers[w].init_context = context` before `thread.start(sched.workers[w])`.
  - [x] Update `scheduler_worker_proc` (`actor/actor.odin:484-558`) to bind `context.allocator = s.allocator` and call `free_all(context.temp_allocator)` after each top-level `a.step_proc` turn; remove `context.user_ptr` clobbering and remove `cast(^Context)context.user_ptr` fallback in `current_context()` (`line 49`).

- [x] Task: Fix Dekker unschedule, worker parking, and pool contention (`actor/actor.odin`)
  - [x] Fix Dekker unschedule ordering in both `scheduler_worker_proc` (`lines 545-552`) and `scheduler_step_one_cooperative` (`lines 1490-1495`): store `a.scheduled = 0` (`.Seq_Cst`), probe `still_has_more := a.step_proc(a, &ctx, 0)`, and only decrement `s.active_actors` (`.Seq_Cst`) when `!still_has_more` or when losing re-schedule CAS.
  - [x] Fix `RunQ_Shard.tail` / parking `.Seq_Cst` ordering (`lines 523-531`) so idle workers park when all shards are empty instead of busy-spinning while 1 active actor executes elsewhere.
  - [x] Separate worker pools (`0 ..< s.num_workers`) from the dedicated `.busy`-locked external pool (`pools[s.num_workers]`) in `scheduler_get_or_create_pools` (`lines 367-372`), and distribute external sends round-robin across `shards` instead of pinning to `shards[0]`.
  - [x] Set `e.pool_id = 0xFFFF` on heap fallback in `acquire_auto_envelope` (`line 972`) and `acquire_auto_envelope_chain` (`lines 1006, 1013`), validate `ctx.sched == target.sched` in `send_ctx*` / `forward_*`, and drain remaining mailbox envelopes in `actor_destroy` (`lines 856-876`).

- [x] Task: Harden `Call_Slot($Res)` layout, UAF-free wakeup, and cycle detection (`actor/actor.odin`)
  - [x] Reorder `Call_Slot($Res)` (`lines 89-92`) to `{ ready: sync.Futex, res_type: typeid, result: Res }` so `ready` is at offset 0.
  - [x] Implement UAF-safe waiter handshake in `call_sched_pid` (`lines 1507-1540`) and `reply_ctx` (`lines 1442-1455`) using states (`0 = Pending`, `1 = Caller_Parked`, `2 = Replied`) and stack-safe completion before caller pops `slot`; validate `slot.res_type == typeid_of(Res)`; clear `ctx.current_reply_slot = nil` and call flags in `reply_ctx` and on the forwarding `Context` in `forward_*` while preserving `env.reply_slot` and `env.flags` on the forwarded `Envelope` to support synchronous `call`/`try_call` delegation.
  - [x] Add `a.base.in_step` cycle detection and `@(thread_local) _tls_call_depth` cap (`MAX_COOPERATIVE_CALL_DEPTH :: 32`) in `scheduler_step_one_cooperative` / `call_sched_pid`.

- [x] Task: Verify Phase 2 tests and all existing runtime tests pass
  - [x] Run `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors`.
  - [x] Run `odin test ./tests` and verify `0` failures and `0` leaks.

- [x] Task: Conductor - User Manual Verification 'Phase 2: Scheduler, Context, Dekker & Call_Slot Hardening' (Protocol in workflow.md)

---

## Phase 3: AST Codegen (`codegen.odin`), Examples, Benchmarks & Docs Alignment

- [x] Task: Write failing AST codegen tests for parser/emitter defects in `tests/codegen_test.odin`
  - [x] Update `test_01` (`tests/codegen_test.odin:46`) to expect `<Actor>_PID :: actor.PID(<Actor>_Msg)` and verify DSL actors can async-reply to each other.
  - [x] Add regression checks covering: behavior parameters named `target` or `reply_to`, backtick attribute `` @(behavior=`reset`) ``, type-inferred default parameters (`memo := "cash"`), blank `_` parameters, polymorphic `$T` and `#+private file` exclusions, aliased `actor` imports, and `*_test.odin` exclusion in `generate_directory`.

- [x] Task: Fix `actor/codegen/codegen.odin` parser and emitter bugs
  - [x] Emit `<Actor>_PID :: actor.PID(<Actor>_Msg)` (`line 458`) and rename generated wrapper target/reply parameters (`_sleipnir_target`, `_sleipnir_reply_to`) to avoid user parameter collisions.
  - [x] Update `get_attribute` to trim both `"` and `` ` ``; skip polymorphic (`$T`), blank (`_`), and `#+private file` procedures properly; handle inferred defaults cleanly; fix `has_actor_import` and token-boundary matching in `is_import_referenced`.
  - [x] Filter out `*_test.odin` in `generate_directory`.

- [x] Task: Fix `examples/library_demo.odin`, `benchmarks/`, `ols.json`, and `README.md` / `benchmarks/README.md`
  - [x] Fix LIFO `defer` ordering in `examples/library_demo.odin` (`lines 72, 91, 171-186, 252-278`) and `benchmarks/actor_benchmark.odin` (`lines 1148-1339`), plus `ws_worker_proc` / `a2a_worker_proc` Dekker ordering and `&s.shards[wid]` indexing.
  - [x] Update `ols.json` with `"checker_args": "-custom-attribute:actor,behavior,no_behavior"`.
  - [x] Fix `README.md:192` (`quota_per_lane = 64, state = &total`) and `benchmarks/README.md` (`actor/spsc.odin`, `RunQ_Shard`, `collector_flush`).

- [x] Task: Regenerate `examples/05_ast_metaprogramming/actors_gen.odin` and run full verification
  - [x] Run `odin run tools/sleipnir_gen -- examples/05_ast_metaprogramming`.
  - [x] Run `odin test ./tests`.
  - [x] Run `odin test examples/05_ast_metaprogramming -custom-attribute:actor,behavior,no_behavior`.
  - [x] Run `odin run examples/library_demo.odin -file -o:speed` and `odin check benchmarks/actor_benchmark.odin -file`.

- [x] Task: Conductor - User Manual Verification 'Phase 3: Codegen, Examples, Benchmarks & Docs Alignment' (Protocol in workflow.md)
