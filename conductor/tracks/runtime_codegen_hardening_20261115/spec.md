# Track Specification: Baseline Runtime & Codegen Concurrency Hardening

**Track ID:** `runtime_codegen_hardening_20261115`
**Type:** Bugfix / Concurrency & Memory Safety Hardening
**Primary Reference:** [`OTP carbon review.md`](../otp_supervision_20261115/references/OTP%20carbon%20review.md)

---

## 1. Objective

Harden Sleipnir's core runtime (`actor/mpsc.odin`, `actor/spsc.odin`, `actor/pool.odin`, `actor/actor.odin`), AST metaprogramming generator (`actor/codegen/codegen.odin`), examples, benchmarks, editor config, and docs so that:
- Zero atomic/non-atomic data races exist on `MPSC_Queue`, `RunQ_Shard`, `Envelope_Pool`, `Watermark_Slab`, or `Scheduler`.
- `actor.call` / `actor.reply` is free of stack use-after-free bugs, places `sync.Futex` at offset 0 of `Call_Slot($Res)` for type-erased wake/cancellation, detects self-calls and cycles in 1-worker cooperative mode, and ignores duplicate `actor.reply` calls within a single turn.
- Worker threads inherit caller `context` (`context.allocator`, `context.logger`, `context.assertion_failure_proc`) and wipe `context.temp_allocator` cleanly at top-level worker turn boundaries without wiping nested cooperative caller scratch frames.
- `Actor_Base` gains lock-free/cache-line-aligned status and generation fields in its existing 64-byte Cache Line 0 tail padding to support Tracks 2–4 OTP supervision cleanly.
- `tools/sleipnir_gen` (`actor/codegen/codegen.odin`) emits valid `<Actor>_PID :: actor.PID(<Actor>_Msg)` aliases, avoids name collisions, preserves file-scoped imports, excludes `*_test.odin`, and handles inferred defaults, backticks, and multi-return procedures safely.

---

## 2. Scope & Detailed Technical Requirements

### 2.1 Lock-Free Mailbox (`actor/mpsc.odin`) & SPSC (`actor/spsc.odin`) Safety

1. **Atomic `q.tail` Stores in `mpsc_pop_detached` & `mpsc_pop_dummy` (`actor/mpsc.odin:76, 82, 102, 116`):**
   - **Defect:** `shard_pop_or_steal` (`actor/mpsc.odin:139`) performs a lock-free pre-check using `intrinsics.atomic_load_explicit(&s.tail, .Relaxed)` concurrently with a shard owner mutating `q.tail` via plain non-atomic assignments (`q.tail = next`) inside `mpsc_pop_detached` and `mpsc_pop_dummy`. Under TSan/LLVM, concurrent non-atomic store + atomic load is an undefined-behavior data race.
   - **Requirement:** Replace every write to `q.tail` after `mpsc_init` (`lines 76, 82, 102, 116`) with `intrinsics.atomic_store_explicit(&q.tail, next, .Relaxed)`.
   - **Note on `shard_init` / `RunQ_Shard` Alignment:** Ensure `RunQ_Shard($T)` keeps `#align(64)` and `mpsc_init` initializes `q.stub` cleanly.

2. **`stub.pool_id = 0xFFFF` Sentinel Initialization (`actor/mpsc.odin:29-33`):**
   - **Defect:** `mpsc_init` leaves `q.stub` zero-initialized (`pool_id == 0`). If a consumer using `mpsc_pop_dummy` pops `&q.stub` as the first `retired_stub`, any collector or watermark table expecting `pool_id` miscredits producer/pool `0` (inflating `Producer_Watermark.retired_count` by 1 and enabling premature ring wrap-around).
   - **Requirement:** Set `q.stub.pool_id = 0xFFFF` and `q.stub.flags = ENVELOPE_FLAG_STATIC` (when `T` has `pool_id`/`flags` or via explicit initialization helpers / documented contract on `Envelope`) so dummy sentinel retirement never impersonates pool `0`.

3. **Move-Safety Documentation & Regression Test (`actor/mpsc.odin:29-33`):**
   - **Requirement:** Document clearly on `MPSC_Queue` and `Actor` that `mpsc_init` stores interior pointers (`q.head = &q.stub`, `q.tail = &q.stub`), so initialized queues and actors are non-movable (must not be moved by value or placed directly inside a resizing `[dynamic]Actor`).

4. **`spsc_consume_batch` Non-Positive `max_batch` Guard (`actor/spsc.odin:71-96`):**
   - **Defect:** When `max_batch <= 0`, `w - r < u64(max_batch)` underflows in unsigned `u64` arithmetic (`u64(max_batch)` wraps to a huge value if negative, or `avail > u64(max_batch)` compares against `0` cleanly only when `max_batch == 0`, while negative `max_batch` clamps `avail` to `u64(-N)` and overflows the ring read loop).
   - **Requirement:** Add `if max_batch <= 0 { return 0 }` as the first executable statement of `spsc_consume_batch`.

---

### 2.2 Envelope Pool & Watermark Slab Hardening (`actor/pool.odin`)

1. **Zero `reply_slot` and `flags` on Recycled Envelopes (`actor/pool.odin:93-135, 256-304`):**
   - **Defect:**
     - `pool_acquire` (`lines 96-99, 106-109, 123-126, 131-134`) resets `env.next`, `env.pool_id`, and `env.flags`, but leaves `env.reply_slot` stale from a prior `actor.call` or `reply_to` send.
     - `watermark_slab_acquire` (`lines 267-270`) and `watermark_slab_acquire_chain` (`lines 289-302`) overwrite `pool_id` (and `msg`) without clearing `flags` or `reply_slot`.
   - **Requirement:** Zero `env.reply_slot = nil` and `env.flags = ENVELOPE_FLAG_NONE` (except `ENVELOPE_FLAG_HEAP` on heap allocation) across every branch of `pool_acquire`, `watermark_slab_acquire`, and `watermark_slab_acquire_chain`.

2. **`Watermark_Slab` Chain Overflow / Dummy-Stub Guard (`actor/pool.odin:276-304`):**
   - **Defect:** `watermark_slab_acquire_chain` allows callers to request `k >= N - 2`, in which case `(s.cursor + u64(k)) - s.cached_retired >= u64(N - 2)` is permanently true even when `s.cursor == s.cached_retired` (unconditional infinite spin), and pushing `k >= N - 1` violates the 2-slot dummy-tail safety margin.
   - **Requirement:** Assert `k > 0 && k < N - 2` (or reject `k <= 0` returning `nil, nil` and clamp/assert `k < N - 2` so at least 2 slots remain reserved for the active dummy tail and retire sentinel).

3. **`pool_init` Zero/Negative `slab_capacity` Deadlock Guard (`actor/pool.odin:59-77`):**
   - **Defect:** `if slab_capacity > 0` leaves `p.slab == nil` when `slab_capacity <= 0`. If `heap_fallback == false`, the first `pool_acquire(p)` spins forever.
   - **Requirement:** If `slab_capacity <= 0`, either force `p.heap_fallback = true` when `heap_fallback` was `false` or clamp to `DEFAULT_AUTO_POOL_CAPACITY` / return early safely so `pool_acquire` never deadlocks on an empty non-fallback pool.

4. **Heap-Fallback Exhaustion Handling (`actor/pool.odin:112-137`, `actor/actor.odin:1149-1239`):**
   - **Defect:** Calling `send_batch_from_pool` with `len(msgs) > slab_capacity` or exhausting a worker pool when `heap_fallback == false` while holding a worker thread spins forever in `pool_acquire(..., spin_if_exhausted = true)` and starves consumer actors.
   - **Requirement:** Ensure `auto_pools` use `heap_fallback = true` (or external/worker `send_ctx` avoids infinite self-spin when called from a scheduler worker thread by falling back or yielding), and document bounded-pool invariants for explicit `Envelope_Pool` callers.

---

### 2.3 Scheduler, Worker Context, Dekker Barrier & `Call_Slot` Hardening (`actor/actor.odin`)

1. **`Actor_Base` Lifecycle & Status Fields (`actor/actor.odin:68-76`):**
   - Extend `Actor_Base` (preserving `#align(64)` and offset-0 `next: ^Actor_Base`) using its Cache Line 0 tail bytes and adding clean supervision metadata pointers (`status: u32`, `in_step: u32`, `generation: u32`, etc.) ready for Tracks 2–4 while keeping `Scheduler` runqueue pops fast and false-sharing free.

2. **Worker Thread Context Inheritance & Turn Temp Allocator Discipline (`actor/actor.odin:283-292, 484-498, 538-557`):**
   - **Defect:** `scheduler_start` (`lines 288-290`) does not copy the calling thread's `context` onto `sched.workers[w].init_context`. Consequently, actor handlers run under `runtime.default_context()` instead of the test/app `context.allocator` (`Tracking_Allocator`), `context.logger`, and `context.assertion_failure_proc`. Additionally, `current_context()` (`line 49`) falls back to `cast(^Context)context.user_ptr`, which crashes if a user sets `context.user_ptr` for non-Sleipnir use on a non-actor thread.
   - **Requirement:**
     - Set `sched.workers[w].init_context = context` before `thread.start(sched.workers[w])` in `scheduler_start`.
     - Inside `scheduler_worker_proc`, bind `context.allocator = s.allocator` and call `free_all(context.temp_allocator)` at the **outer worker loop boundary** after each top-level turn (`line 539`)—and NEVER inside `scheduler_step_one_cooperative`, so caller scratch allocations in `context.temp_allocator` survive nested cooperative `actor.call` turns.
     - Remove `return cast(^Context)context.user_ptr` in `current_context()` (`line 49`); return `_tls_active_ctx` strictly (or `nil`), and do not hijack `context.user_ptr` inside `scheduler_worker_proc`.

3. **`Call_Slot($Res)` Layout, Type Validation & Stack UAF Elimination (`actor/actor.odin:89-92, 1442-1455, 1507-1540`):**
   - **Defect 1 (Offset-`size_of(Res)` `Futex`):** `Call_Slot($Res)` puts `result: Res` at offset 0 and `ready: sync.Futex` after it, preventing type-erased cancellation `(^sync.Futex)(slot_ptr)` in runtime cleanup or panic recovery.
   - **Defect 2 (Stack UAF & Unconditional Syscall):** In `reply_ctx` (`lines 1449-1450`), writing `1` to `slot.ready` lets a spinning caller in `call_sched_pid` (`line 1522`) return immediately and pop/reuse its stack frame *before* `reply_ctx` executes `sync.futex_signal(&slot.ready)` on line 1450. It also performs an unnecessary `futex_signal` syscall even when the caller is spinning and never parked.
   - **Defect 3 (Double-Reply & Missing Type Check):** Calling `actor.reply` twice within the same turn mutates a popped `Call_Slot` or sends duplicate messages, and there is no runtime check that `reply` matches `$Res`.
   - **Requirement:**
     - Reorder `Call_Slot($Res)` so `ready: sync.Futex` is at **offset 0**, followed by `res_type: typeid` and `result: Res`.
     - Replace the 2-state protocol (`0`/`1`) with an atomic waiter-handshake (`0 = Pending`, `1 = Caller_Parked`, `2 = Replied`, `3 = Faulted/Stopped`):
       - Caller spins, then atomically transitions `0 -> 1` (`atomic_compare_exchange_strong_explicit(..., 0, 1, .Acq_Rel, .Acquire)`) only before calling `sync.futex_wait(&slot.ready, 1)`.
       - `reply_ctx` writes `slot.result` (checking `slot.res_type == typeid_of(Res)`), atomically swaps `prev := atomic_exchange_explicit((^u32)(&slot.ready), 2, .Acq_Rel)`, and invokes `sync.futex_signal(&slot.ready)` **if and only if `prev == 1`** (Wait! Ensure lifetime safety even on `prev == 1 -> 2` wakeup: either use a completion flag/handshake or `futex_signal` after setting a stack-safe state where caller waits until worker clears/acknowledges `signaling` bit before returning from `call_sched_pid`—zero UAF window guaranteed!).
     - Clear `ctx.current_reply_slot = nil` and `ctx.current_flags &~= (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)` at the start of `reply_ctx` so duplicate `reply` calls are safe no-ops. In `forward_*`, clear `ctx.current_reply_slot = nil` and `ctx.current_flags &~= (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)` on the **forwarding actor's stack `Context`** (so ending the forwarding turn or calling `reply` after `forward` is a safe no-op), while **preserving `env.reply_slot` and `env.flags` (`ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID`) on the forwarded `Envelope`** so downstream targets complete delegated `call`/`try_call` promises directly (or break them on crash).
     - Verify/wake any unreplied `ENVELOPE_FLAG_CALL` slot cleanly if an actor finishes a turn without calling `reply` or if the actor is destroyed.

4. **Dekker Unschedule Lost-Wakeup & Shard Steal Races (`actor/actor.odin:390-405, 545-552, 1490-1495`):**
   - **Defect:** In `scheduler_worker_proc` (`lines 545-551`) and `scheduler_step_one_cooperative` (`lines 1490-1495`), `s.active_actors` is decremented with `.Acq_Rel` *before* probing `still_has_more`! This creates a window where `active_actors` transiently hits `0` while messages are still sitting in `a.mailbox`, causing `scheduler_wait_idle` (`line 411`) to return prematurely!
   - **Requirement:**
     - Probe `still_has_more` **before** calling `intrinsics.atomic_sub_explicit(&s.active_actors, 1, .Seq_Cst)` (i.e.: store `a.scheduled = 0` with `.Seq_Cst`, probe `still_has_more := a.step_proc(a, &ctx, 0)`, if `still_has_more`: re-acquire `a.scheduled` via `atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0`—re-pushing to shard without decrementing `active_actors` if we won the exchange, or decrementing `active_actors` only if we lost or `!still_has_more`).
     - In `scheduler_worker_proc` parking (`lines 523-529`), use `.Seq_Cst` store/exchange for `parked_workers` and check `active_actors == 0` OR all shards empty (so an idle worker doesn't busy-spin at 100% CPU when `num_workers > 1` and only 1 actor is running on another worker).

5. **1-Worker Cooperative `call` Cycle Detection & Depth Cap (`actor/actor.odin:1470-1527`):**
   - **Defect:** Calling `actor.call(self, ...)` or mutual `A -> B -> A` synchronous calls on `num_workers == 1` hangs forever or blows the stack via unbounded mutual recursion of `scheduler_step_one_cooperative`.
   - **Requirement:**
     - Track `in_step: u32` on `Actor_Base` (set to `1` around `a.step_proc`) and TLS `@(thread_local) _tls_call_depth: int` (`MAX_COOPERATIVE_CALL_DEPTH :: 32`).
     - If `target.in_step != 0` on a single-worker scheduler (or cooperative loop detects cycle), abort/fail safely instead of infinite spin.
     - Restore `_tls_active_ctx` with `defer` on cooperative call/step paths so unwinds never corrupt TLS.

6. **Pool Separation, Heap Fallback `pool_id = 0xFFFF`, `send_ctx` Scheduler Validation & `scheduler_init` OOM Rollback (`actor/actor.odin:179-210, 355-387, 856-876, 946-1020, 1189-1239, 1303-1388`):**
   - **Worker vs External Pool Isolation:** Allocate `s.num_workers + 1` pools (`pools[0 ..< s.num_workers]` strictly uncontended per worker; `pools[s.num_workers]` protected by `.busy` CAS lock for external threads), and round-robin `shard_hint` across external sends instead of pinning 100% of external traffic onto `shards[0]`.
   - **Heap-Fallback `pool_id`:** Set `e.pool_id = 0xFFFF` (not `0`) in `acquire_auto_envelope` (`line 972`) and `acquire_auto_envelope_chain` (`lines 1006, 1013`), and guard `collector_add` / `step_proc` / drain loops so `ENVELOPE_FLAG_HEAP` nodes are freed with the scheduler/actor allocator rather than returned to `pools[0]`.
   - **`send_ctx` Scheduler Check:** Check `ctx.sched == target.sched` (or `target.ref.sched`) in `send_ctx` / `send_ctx_pid` / `send_ctx_batch` / `send_ctx_batch_pid` and `forward_*` so cross-scheduler sends fall back to `target.sched`'s external path instead of touching `target.pools[wid]` unsafely. On cross-scheduler `forward`, copy the payload onto a target-owned envelope and clear `ctx.forwarded` so the source envelope is recycled locally.
   - **Mailbox Drain on Actor Destroy:** In `actor_destroy` (`line 856`), drain remaining mailbox envelopes so `ENVELOPE_FLAG_HEAP` envelopes are freed cleanly, and unregister/mark destroyed safely so calling `actor.destroy(a)` after `scheduler_stop_and_destroy` does not double-free or read freed scheduler memory (e.g. ensure user-held actors vs owned destruction contract is safe, or clear/guard cleanly).
   - **`scheduler_init` Fixes (`lines 179-210`):** Save `prev_sched := _tls_active_sched` and roll back partial allocations on error; respect explicit `0 < cfg.num_shards < s.num_workers` instead of silently clamping up to `s.num_workers`; remove dead `target_messages` field if unused or wire it cleanly.
   - **Unbounded Wait Sleep Optimization:** Avoid 100% CPU `cpu_relax()` burn in `wait_idle`, `wait_tokens`, and `wait_messages` when `enable_parking == true`.

---

### 2.4 AST Codegen (`actor/codegen/codegen.odin`), Examples, Benchmarks & Config Fixes

1. **`actor/codegen/codegen.odin` Fixes:**
   - **Emit Valid `<Actor>_PID`:** Change line 458 from `<Actor>_PID :: <Actor>_Actor` to `<Actor>_PID :: actor.PID(<Actor>_Msg)` (and update `tests/codegen_test.odin:46` to expect `Bank_Account_PID :: actor.PID(Bank_Account_Msg)` so DSL actors can forward async `reply_to` PIDs to each other!).
   - **Parameter Name Collisions (`lines 480-558`):** Rename generated wrapper target/reply parameter names to `_sleipnir_target` / `_sleipnir_reply_to` (or disambiguate when user params use `target` or `reply_to`).
   - **Inferred Types, Blank `_`, & Backticks (`lines 105-120, 290-315, 420-480`):**
     - Trim both `"` and `` ` `` in `get_attribute`.
     - Synthesize positional field names (`_arg0`, `_arg1`) for blank `_` parameters.
     - Derive field types from default literal expressions when `field.type == nil`, or emit a clear diagnostic instead of invalid `memo: ,`.
   - **Import Matching & File Scoping (`lines 195-215, 340-372`):**
     - Fix `actor` import discovery to recognize exact `"actor"`, `".../actor"`, and aliased `import actor "..."` declarations, avoiding broken fallback `import "actor"`.
     - Replace substring `strings.contains(..., "alias.")` in `is_import_referenced` with token/identifier-boundary matching so `-vet` never fails on spurious imports, and avoid dropping distinct imports sharing an alias across different files.
   - **Directory Filtering & Procedure Filtering (`lines 215-275, 670-685`):**
     - Skip `*_test.odin` and non-matching OS/build-tag files in `generate_directory`.
     - Skip polymorphic procedures (`$T`), `#+private file` items, and duplicate `@(behavior="...")` definitions with clear diagnostics.
     - Support multi-return behaviors `-> (A, B)` cleanly via tuple structs or destructuring in `reply` / `call`, or report a clear compile-safe error.

2. **`ols.json` & `.github/workflows/ci.yml`:**
   - Add `"checker_args": "-custom-attribute:actor,behavior,no_behavior"` to `ols.json` (expanded to `supervise,reinit` in Tracks 2 and 4).

3. **`examples/library_demo.odin`, `benchmarks/actor_benchmark.odin`, `README.md` & `benchmarks/README.md`:**
   - Fix LIFO `defer` ordering in `examples/library_demo.odin` (`lines 72, 91, 171-186, 252-278`) and `benchmarks/actor_benchmark.odin` (`lines 1148-1339`) so `scheduler_stop_and_destroy` runs *after* workers stop reading slices (`actors`, `pools`, `tokens`) and *after* actor allocations remain valid.
   - Fix `ws_worker_proc` & `a2a_worker_proc` Dekker store/probe ordering and shard indexing (`&s.shards[wid]` not `[wid * 2]`) in `benchmarks/actor_benchmark.odin`.
   - Fix `README.md:192` (`quota_per_lane = 64, state = &total`) and `benchmarks/README.md` (`actor/spsc.odin`, `RunQ_Shard` / `collector_flush` / `shard_hint`).

---

## 3. Acceptance Criteria

1. `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors` and `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors` pass with zero warnings or errors.
2. `odin test ./tests` passes all existing tests plus newly added concurrency/regression tests (`.Seq_Cst` Dekker unschedule stress, heap-fallback `pool_id = 0xFFFF`, recycled `reply_slot == nil`, `spsc_consume_batch` `max_batch <= 0`, `Call_Slot` stack UAF stress, `Tracking_Allocator` visibility inside worker handlers, `1`-worker cooperative cycle detection, and codegen edge cases) with `0` leaks and `0` bad frees.
3. `odin test examples/05_ast_metaprogramming -custom-attribute:actor,behavior,no_behavior` and `odin run examples/library_demo.odin -file -o:speed` pass cleanly with zero UAF/double-free crashes.
