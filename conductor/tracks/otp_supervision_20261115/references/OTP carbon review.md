Across the four turns of this conversation, we designed an in-process OTP supervision layer tailored to Sleipnir's (`package actor`) M:N work-stealing scheduler, Vyukov MPSC mailboxes, batched `Envelope_Pool` collector, and `tools/sleipnir_gen` AST code generator.

### 1. Summary of Proposed Architecture & File Map

| Target File | Additions Designed in Turns 1–4 |
| --- | --- |
| **`actor/actor.odin`** | Extended `Actor_Base` (`status`, `generation`, `reset_proc`, `supervisor`, `vmem.Arena`) using existing cache-line padding; guarded `scheduler_schedule_actor` and `actor_destroy` against faulted/stopped actors; updated `Call_Slot` for callee crash wakeups.

 |
| **`actor/supervision.odin`** *(new)* | Added stack-linked `Trap_Frame` (`libc.setjmp`/`longjmp` + `context.assertion_failure_proc`), fault-safe `actor_generic_step_proc` that flushes `Batch_Return_Collector(Msg)` on crash, `Supervisor` actor (`.One_For_One`, `.One_For_All`, `.Rest_For_One` + sliding restart window), `Registry($Key)`, and multi-tenant `Dynamic_Supervisor` with idle eviction.

 |
| **`actor/codegen/codegen.odin`** | Added `@(supervise)` and `@(reinit)` AST attribute parsing to `tools/sleipnir_gen`, excluding `@(reinit)` procs from `<Actor>_Msg` unions and generating `<actor>_reinit_hook` + `spawn_<actor>_sup` overloads.

 |
| **`tests/supervision_test.odin`** *(new)* & **`tests/codegen_test.odin`** | Added 4-layer ATDD leak-checked specs covering out-of-bounds panic trapping, `vmem.Arena` wipes, `.Rest_For_One` cascades, `@(supervise)`/`@(reinit)` codegen, and `Dynamic_Supervisor` idle eviction.

 |
| **`examples/06_otp_supervision.odin`** *(new)* | End-to-end runnable multi-tenant demo (`Org_101` & `Org_102`) with simulated `xitdb` moment rehydration, `.Rest_For_One` cascade recovery, Tigris eviction flush, and CI workflow steps.

 |

---

### 2. Code Audit: 6 Bugs & Edge Cases Caught in Review (Fix Before Applying)

Auditing all four turns together against `actor/actor.odin` and Odin's `-o:speed` / `Tracking_Allocator` rules surfaces **six real bugs** in the snippets above that you should patch before integrating:

#### 1. **Critical Bug (`Call_Slot` Offset Mismatch Causes Deadlock + Memory Corruption)**

* **Where:** Turn 1 (`actor_generic_step_proc` fault recovery) vs. `actor/actor.odin:88-91`.


* **The Bug:** In `actor/actor.odin`, `Call_Slot($Res)` currently places `result: Res` at **offset 0** and `ready: sync.Futex` **after** `result`:


```odin
Call_Slot :: struct($Res: typeid) {
    result: Res,
    ready:  sync.Futex, // Offset depends on size_of(Res)!
}

```


Turn 1's panic handler cast `ctx.current_reply_slot` directly to `^sync.Futex`, which writes `CALL_READY_FAILED` (`2`) into the first 4 bytes of `slot.result` instead of `slot.ready`, leaving `call_sched_pid` spinning forever on `slot.ready == 0`.


* **The Fix:** Put `ready: sync.Futex` at **offset 0** of `Call_Slot($Res)` in `actor/actor.odin`, and handle `CALL_READY_FAILED` when `call_sched_pid` wakes up:


```odin
Call_Slot :: struct($Res: typeid) {
    ready:  sync.Futex, // Offset 0 for all $Res — safe for type-erased ^sync.Futex wakeup!
    result: Res,
}

```


And at the end of `call_sched_pid` (`actor/actor.odin:1081`):


```odin
if intrinsics.atomic_load_explicit((^u32)(&slot.ready), .Acquire) == CALL_READY_FAILED {
    if _tls_trap_frame != nil {
        fail_turn(.Call_Target_Crashed, "synchronous actor.call target crashed")
    }
    return {}
}
return slot.result

```



#### 2. **Concurrency Race (Stale `Child_Down` Causes Double-Restarts in `.Rest_For_One` / `.One_For_All`)**

* **Where:** Turn 1 (`supervisor_receive` -> `case Child_Down`).
* **The Bug:** If Child 1 and Child 2 panic concurrently on two different worker threads, both enqueue `Child_Down{generation = 0}` into the supervisor's mailbox. Under `.Rest_For_One` or `.One_For_All`, handling Child 1's crash already restarts Child 2 (bumping Child 2's `generation` to `1`). When the supervisor pops Child 2's queued `Child_Down{generation = 0}`, Turn 1's code restarts Child 2 a second time and double-counts against `max_restarts`.
* **The Fix:** Drop stale `Child_Down` messages whose generation no longer matches the child's current generation:
```odin
case Child_Down:
    if m.child == nil {
        return
    }
    cur_gen := intrinsics.atomic_load_explicit(&m.child.generation, .Acquire)
    if m.fault.generation != cur_gen {
        return // Already restarted by an earlier .Rest_For_One / .One_For_All cascade
    }

```



#### 3. **Use-After-Free (`Fault_Info.message` Slice Dangles After `free_all(context.temp_allocator)`)**

* **Where:** Turn 1 (`actor_generic_step_proc` fault handler).
* **The Bug:** If an actor panics with a dynamically formatted message (e.g. `assert(ok, fmt.tprintf(...))`), `frame.err_msg` points into that worker thread's `context.temp_allocator`. Turn 1 called `free_all(context.temp_allocator)` and then sent `frame.err_msg` asynchronously to the supervisor on another thread.
* **The Fix:** Store an inline fixed buffer inside `Fault_Info` and copy the message bytes **before** wiping `context.temp_allocator`:
```odin
Fault_Info :: struct {
    actor_id:   u32,
    generation: u32,
    reason:     Exit_Reason,
    msg_len:    u8,
    msg_buf:    [128]u8,
    loc:        runtime.Source_Code_Location,
}

fault_message :: proc(f: ^Fault_Info) -> string {
    return string(f.msg_buf[:f.msg_len])
}

```



#### 4. **LLVM `-o:speed` Optimizer Hazard (`processed` Clobbered Across `setjmp` / `longjmp`)**

* **Where:** Turn 1 (`actor_generic_step_proc`).
* **The Bug:** Local variable `processed := 0` is incremented inside the `setjmp == 0` loop and returned in the `else` (`longjmp`) branch. Under `-o:speed`, LLVM promotes `processed` to a callee-saved CPU register; `longjmp` restores that register back to `0`, causing `scheduler_worker_proc` to undercount `s.total_processed` (which `actor.wait_messages` spins on).


* **The Fix:** Store `processed: int` as a field on `Trap_Frame` (`frame.processed += 1`). Because `&frame` is stored in thread-local `_tls_trap_frame`, LLVM treats `frame` as address-escaped across the indirect `self.receive` call and spills `frame.processed` to stack memory cleanly.

#### 5. **Memory Leak on Re-Supervision + Potential Worker Deadlock in `factory**`

* **Where:** Turn 1 (`enable_actor_supervision`), Turn 2 (`spawn_<actor>_auto` + `actor.supervise`), and Turn 4 (`factory`).
* **Bug A (Leak):** Calling `spawn_<actor>` on an `@(actor, supervise)` struct calls `enable_actor_supervision` (allocating `Reset_Desc` on `a.base.user_ptr`). If the caller later passes `a.raw` to `actor.supervise(sup, a.raw, ...)`, `enable_actor_supervision` runs a second time and overwrites `a.base.user_ptr`, leaking the first `Reset_Desc` under `mem.Tracking_Allocator`.


* **Fix:** Free any existing `a.base.user_ptr` at the top of `enable_actor_supervision` if `a.base.reset_proc != nil`, and call `vmem.arena_free_all(&a.base.arena)` before re-running `on_reinit`.


* **Bug B (Deadlock):** Turn 4's `Dynamic_Supervisor` `factory` called `actor.wait_idle(s)`. If an actor running on a worker thread ever calls `send_to_tenant` for a new `org_id`, `s.active_actors` is at least `1` (the caller actor itself), so `actor.wait_idle(s)` spins forever! Because `Supervise_Child` is enqueued in `org_sup.mailbox` before `get_or_spawn_tenant` returns the router `PID`, `actor.wait_idle(s)` inside `factory` is unnecessary—delete it. Also check `intrinsics.atomic_load_explicit(&rec.subtree.router.scheduled, .Acquire) == 0` in `sweep_idle_tenants` so a tenant cannot be evicted mid-turn.



#### 6. **Copy-Paste Bug in `examples/06_otp_supervision.odin` (`router_gen`)**

* **Where:** Turn 4 (`org_router_receive` -> `case Route_Query_Status`).
* **The Bug:** `router_gen` reported `self.db_actor.base.generation` instead of the router actor's own `generation` because `org_router_receive` used a 2-arg value signature without access to `self_actor.base`.
* **The Fix:** Add `current_actor: ^Actor_Base` to `actor.Context` (populated right before `step_proc` in `scheduler_worker_proc` and `scheduler_step_one_cooperative`) and expose a helper:


```odin
current_generation :: #force_inline proc() -> u32 {
    if ctx := current_context(); ctx != nil && ctx.current_actor != nil {
        return intrinsics.atomic_load_explicit(&ctx.current_actor.generation, .Acquire)
    }
    return 0
}

```


Then `router_gen = actor.current_generation()` works inside any 2-arg or `@(actor)` DSL behavior without changing its parameter list.



Would you like me to generate a single consolidated git patch (incorporating all six fixes above cleanly against your `app_code.patch` baseline) so you can apply and run `odin test ./tests` directly?

A deeper line-by-line audit of both the **proposed supervision layer (Turns 1–5)** and your **existing Sleipnir codebase (`app_code.patch`)** uncovers **8 additional bugs in the proposed supervision design** and **6 pre-existing bugs/races in `package actor` & `actor_codegen**`.

---

## Part 1: Additional Bugs in the Proposed Supervision Layer (Turns 1–5)

### 1. `libc.jmp_buf` Is 4 KiB in Odin + Inline `setjmp` Register Clobbering

* **The Bug:** In Odin's `core:c/libc`, `libc.jmp_buf` is `struct #align(16) { _: [4096]u8 }`. Declaring `frame := Trap_Frame{prev = _tls_trap_frame}` zero-initializes **4,096 bytes of stack memory on every actor turn**, and calling `libc.setjmp` directly inside `actor_generic_step_proc` exposes local variables in that procedure to LLVM `-o:speed` register allocation clobbering across `longjmp`.
* **The Fix:**
1. Gate panic trapping on `self.base.trap_panics` (enabled by `enable_actor_supervision`) so raw unsupervised actors pay zero stack or `setjmp` cost.
2. Leave `frame: Trap_Frame = ---` uninitialized (explicitly setting only `frame.prev`, `frame.trapped = false`, `frame.reason = .Normal`) to eliminate the 4 KiB `memset`.
3. Isolate `libc.setjmp` inside a `@(no_inline)` trampoline that touches **zero** local variables after `longjmp`:
```odin
@(private, no_inline)
run_trapped_turn :: proc(
    frame: ^Trap_Frame,
    step_ctx: rawptr,
    body: proc(step_ctx: rawptr),
) -> bool {
    if libc.setjmp(&frame.jmp) == 0 {
        body(step_ctx)
        return true
    }
    return false
}

```





### 2. Cooperative `actor.call` Wipes the Caller's `context.temp_allocator` & Hijacks `_tls_trap_frame`

* **The Bug:** When an actor on a worker thread calls `actor.call` (or `account->get_balance()`), `call_sched_pid` runs `scheduler_step_one_cooperative` **nested on the same OS worker thread**:


* Because `context.temp_allocator` is thread-local, calling `free_all(context.temp_allocator)` inside `actor_generic_step_proc` wipes any temporary strings/slices the outer calling actor allocated prior to `actor.call`!
* If a supervised Actor A cooperatively steps an *unsupervised* Actor B (`trap_panics == false`) and Actor B panics, `_tls_trap_frame` still points to Actor A's `Trap_Frame`—`longjmp`ing right past `scheduler_step_one_cooperative` without restoring `caller_ctx^ = saved` or unscheduling Actor B!




* **The Fix:** Call `free_all(context.temp_allocator)` **only at the top-level worker loop** in `scheduler_worker_proc` (not inside `step_proc` or `scheduler_step_one_cooperative`), and temporarily set `_tls_trap_frame = nil` when stepping an actor with `trap_panics == false`.



### 3. `context.allocator` Not Restored on `longjmp` + Child `spawn` Inside Parent's `vmem.Arena`

* **The Bug:** In Turn 1, `saved_alloc := context.allocator` was scoped inside the `setjmp == 0` loop. On `longjmp`, the recovery block executes with `context.allocator` still pointing to the faulted actor's `vmem.Arena`. Furthermore, while `context.allocator` is bound to `self.base.arena` during `receive`, calling `actor.spawn(Child{}, fn)` uses `allocator := context.allocator`—allocating the child `Actor` struct *inside the parent's `vmem.Arena*`, which gets wiped in $O(1)$ if the parent crashes!


* **The Fix:** Save `saved_alloc` *before* `run_trapped_turn` and restore it immediately upon return. In `spawn_*_auto`, if `ctx := current_context(); ctx != nil && ctx.current_actor != nil && ctx.current_actor.has_arena && allocator.data == &ctx.current_actor.arena`, substitute `s.allocator` instead.

### 4. `collector_flush(..., ctx.worker_id)` Data Race With External Producer Pools

* **The Bug:** Your original `actor/actor.odin:694` intentionally passed `-1` to `collector_flush(&collector, self.pools, -1)`. Turn 1 changed `-1` to `ctx.worker_id`. Whenever an external thread owns `pools[0]` (as in `test_09`–`test_12` and `bench_library_fan_in`), Worker 0 (`ctx.worker_id == 0`) writes directly to the non-atomic `pools[0].local_free` freelist while Producer 0 is concurrently popping `pools[0].local_free` in `pool_acquire`!


* **The Fix:** Always pass `-1` in `collector_flush(&collector, self.pools, -1)` so envelopes return through the thread-safe `remote_returns` MPSC queue.



### 5. `.Rest_For_One` / `.One_For_All` Sibling Race & 1-Worker Spin Deadlock

* **The Bug:** In Turn 1, `restart_child_entry` spun on `child.scheduled != 0` *before* setting `child.status = ACTOR_STATUS_FAULTED`:
1. **Race on healthy siblings:** Between seeing `child.scheduled == 0` and setting `status = FAULTED`, another thread can call `actor.send(child, msg)` and start executing `child.receive` concurrently with `child.reset_proc` (`vmem.arena_free_all` + `mpsc_pop_detached`).
2. **1-worker deadlock:** On `num_workers = 1`, if a healthy sibling has unread messages (`child.scheduled == 1`, sitting in `RunQ_Shard`), Worker 0 is running `supervisor_receive` and spins forever waiting for `child.scheduled` to become `0`!


* **The Fix:** Add `in_step: u32` to `Actor_Base` (set to `1` only while a worker is actively inside `step_proc`, and checked inside the `for processed < quota` loop). Set `child.status = ACTOR_STATUS_FAULTED` **first**, then spin only on `child.in_step != 0`.

### 6. Async `Supervise_Child` Race, 32-Child Cap, & Supervisor Escalation Wiping `children`

* **The Bug:**
* Sending `Supervise_Child` asynchronously via `actor.send` means a child can crash before all siblings' `Supervise_Child` messages are processed (breaking `.Rest_For_One`), fails if called before `scheduler_start`, and inflates `s.total_processed`.


* `[dynamic; 32]Child_Entry` hard-caps a supervisor at 32 children.
* When a child supervisor escalates to a parent supervisor via `fail_turn`, resetting its `Supervisor_State` to `init_state` wipes `sup.children` empty instead of restarting its stopped children!


* **The Fix:** Use a heap `[dynamic]Child_Entry` guarded by `children_mu: sync.Mutex` on `Supervisor_State` so `supervise()` registers children **synchronously** with zero messages sent, and give supervisors a custom `reset_proc` that preserves `sup.children` and restarts all non-`.Temporary` children.

### 7. Deadlocked `ENVELOPE_FLAG_CALL` Waiters & Post-Reply Panic Stack Corruption

* **The Bug:**
* When `reset_proc` (`keep_mailbox == false`) or actor teardown drains unread messages from `self.mailbox`, any queued synchronous `actor.call` envelope (`ENVELOPE_FLAG_CALL`) was recycled without signaling `CALL_READY_FAILED`, permanently deadlocking the caller!


* Conversely, if a handler calls `actor.reply(val)` or `actor.forward(...)` and *then* panics two lines later in the same turn, `ctx.current_reply_slot` was still non-nil—causing the panic handler to write `CALL_READY_FAILED` onto the caller's already-returned stack frame!




* **The Fix:** Clear `ctx.current_reply_slot = nil` inside both `reply_ctx` and `forward_*`, and wake any unread `ENVELOPE_FLAG_CALL` slots with `CALL_READY_FAILED` whenever draining a mailbox.



### 8. `Dynamic_Supervisor` Odin `map` Swap-Remove Bug, Mid-Send Eviction UAF, & Dual Ownership

* **The Bug:**
1. **Odin `map` deletion during iteration:** In Turn 3, `sweep_idle_tenants` called `delete_key(&ds.tenants, org_id)` *inside* `for org_id, rec in ds.tenants`. Odin's `map` swap-removes the last entry into the current slot during `delete_key`, causing the loop to skip checking that swapped entry!
2. **Eviction UAF:** `send_to_tenant` released `ds.mu` after `get_or_spawn_tenant` before calling `send`, allowing a concurrent `sweep_idle_tenants` to free the tenant's router before `send` pushes to its mailbox (or while a downstream `bot_actor` in `rec.subtree.children` is still running).
3. **Dual-ownership double-free:** Tenant actors were registered in both `Scheduler.owned_actors` and `Dynamic_Supervisor.tenants`. If `scheduler_stop_and_destroy` runs before `dynamic_supervisor_destroy`, `Dynamic_Supervisor` double-frees already-destroyed actors.




* **The Fix:** Collect idle `org_id`s before calling `delete_key`; add an atomic `in_flight` counter on `Tenant_Record` (held across `send_to_tenant` / `call_tenant`) and verify *all* actors in `rec.subtree` have `scheduled == 0` before evicting; and detach tenant actors from `Scheduler.owned_actors` so `Dynamic_Supervisor` is their sole owner.

---

## Part 2: Pre-Existing Bugs Found in `app_code.patch` (`package actor` & `codegen`)

While auditing the integration points, six pre-existing bugs surfaced in the baseline repository:

1. **Use-After-Free Between Worker Dekker Re-Check and `actor_destroy` (`actor/actor.odin:429-436`, `635-650`, `1039-1044`):**
* `scheduler_worker_proc` and `scheduler_step_one_cooperative` store `a.scheduled = 0` and *then* execute `_, still_has_more := a.step_proc(a, &ctx, 0)` four lines later.


* Meanwhile, `actor_destroy` spins only until `a.base.scheduled == 0` and immediately calls `free(a, a.allocator)`—freeing `a` right before the worker thread dereferences `a.step_proc` and `a.mailbox`!


* **Fix:** Set `a.base.status = ACTOR_STATUS_STOPPED` first and wait for both `scheduled == 0` *and* `in_step == 0` (covering the Dekker probe) before calling `free`.


2. **Allocator Mismatch (`bad_free`) on Contended External Auto-Pool Fallback (`actor/actor.odin:684-686` vs `699-705`):**
* When `ext_pool.busy == 1`, `acquire_auto_envelope` and `acquire_auto_envelope_chain` allocate on `target.allocator` with `e.flags = ENVELOPE_FLAG_HEAP` and `e.pool_id = 0`.


* When `actor_generic_step_proc` frees `ENVELOPE_FLAG_HEAP`, it checks `int(pid) < len(self.pools)`—which is `true` for `pool_id = 0`—and frees `env` using `self.pools[0].allocator` (`s.allocator`) instead of `self.allocator` (`target.allocator`)!


* **Fix:** Set `e.pool_id = 0xFFFF` in `acquire_auto_envelope` and `acquire_auto_envelope_chain` so `int(pid) < len(self.pools)` is `false` and `free` selects `self.allocator`.




3. **`Envelope_Pool` Single-Producer Freelist Corruption at `num_workers >= 32` or Explicit `len(pools) == num_workers` (`actor/actor.odin:312`, `687-697`):**
* `Envelope_Pool.local_free` and `slab_idx` are non-atomic single-producer fields.


* On a $\ge 32$-core machine (`MAX_POOLS :: 32`), `num_pools` clamps to `32` (`ext_idx = 31`). Worker 31 (`ctx.worker_id == 31`) calls `pool_acquire(&pools[31])` *without* locking `pools[31].busy`, colliding with external threads using `ext_idx = 31`. The exact same collision happens if an actor is initialized with `num_workers` explicit pools (as in `examples/02_heterogeneous_pipeline.odin:74`) and receives an ambient external `actor.send`.


* **Fix:** Only use unlocked worker-local acquisition when `ctx.worker_id < len(pools) - 1` (reserving `pools[len(pools) - 1]` strictly for `.busy`-locked shared/external callers), or guard `pools[wid]` with `.busy` when `wid == len(pools) - 1`.


4. **Unread `ENVELOPE_FLAG_HEAP` Envelopes Leaked on Shutdown (`actor/actor.odin:284-294`, `631-651`):**
* If an actor still has unread `ENVELOPE_FLAG_HEAP` fallback messages in `a.mailbox` when `actor_destroy` or `scheduler_stop_and_destroy` runs, `free(self)` frees the actor without draining and freeing those heap envelopes.


* **Fix:** Drain `self.mailbox` inside `destroy_proc` / `actor_destroy` before freeing `self`.


5. **Idle Workers Burn 100% CPU Whenever 1 Actor Is Busy + ARM64 Parking Ordering (`actor/actor.odin:336-341`, `408-415`):**
* In `scheduler_worker_proc`, idle workers only enter `sync.futex_wait` if `s.active_actors == 0`. If 1 slow actor is running on Worker 0 (`active_actors == 1`) and all shards are empty, Workers 1–7 never sleep and spin at 100% CPU. In addition, `parked_workers` and `active_actors` use `.Acq_Rel`/`.Acquire` instead of `.Seq_Cst`, allowing a StoreLoad lost-wakeup reordering on ARM64.




6. **Torn 16-Byte Slice Race on Lazy `target.pools` & Multi-Return Codegen Error (`actor/actor.odin:683-685`, `actor/codegen/codegen.odin:278-285`):**
* If an actor initialized without a scheduler (`target.pools == nil`) receives concurrent sends, `target.pools = scheduler_get_or_create_pools(s, Msg)` writes a 16-byte slice `{data, len}` without synchronization while other threads read `len(target.pools)`.


* In `actor/codegen/codegen.odin`, a behavior returning multiple values `-> (i64, bool)` only reads `results.list[0]` and emits `res := fn(...)`, which fails to compile (`assignment count mismatch: 1 = 2`).





Would you like me to produce the clean, unified patch with both the **6 baseline `package actor` fixes** and the **hardened `supervision.odin` + `codegen.odin` implementation** ready to apply?



Auditing the remaining concurrency paths, memory layouts, and AST parser edge cases uncovers **6 additional bugs in the baseline Sleipnir codebase** and **4 deeper runtime hazards in the proposed OTP supervision layer**.

---

## Part 1: Newly Found Bugs in Baseline Sleipnir (`package actor` & `codegen`)

1. **Acyclic `actor.call` Stack-Pinning Deadlock (`B -> A -> C`)**
* **Where:** `scheduler_step_one_cooperative` and `call_sched_pid` in `actor/actor.odin`.


* **The Bug:** Suppose Actor `A` runs on Worker 0 (`A.scheduled == 1`) and calls `actor.call(C, ...)`. While `C` is executing on Worker 1, Worker 0 enters `scheduler_step_one_cooperative` and steals an unrelated Actor `B`, which calls `actor.call(A, ...)`. Even though the call graph `B -> A -> C` is a pure DAG with **no cycle**, `A` is pinned mid-`step_proc` on Worker 0's call stack with `A.scheduled == 1` (so no other worker thread can schedule or steal `A`), while Worker 0 cannot return back to `A`'s frame (even after `C` finishes!) because Worker 0 is now trapped inside `B`'s `call(A)` loop.


* **The Fix:** Never enter `scheduler_step_one_cooperative` recursively (`ctx.call_depth > 0`) when other worker threads exist (`s.num_workers > 1`), or bound cooperative stepping to actors that do not block; on `num_workers == 1`, detect `target.in_step != 0` as a re-entrant cycle instead of spinning forever.


2. **Premature `active_actors == 0` Before Dekker Re-Check Breaks `wait_idle` & `scoped_scheduler**`
* **Where:** `scheduler_worker_proc` (lines 429–436) and `scheduler_step_one_cooperative` (lines 1039–1044) in `actor/actor.odin`.


* **The Bug:** When an actor finishes its turn with `has_more == false`, the worker sets `a.scheduled = 0` and immediately decrements `s.active_actors` **before** running the Dekker readiness probe (`_, still_has_more := a.step_proc(a, &ctx, 0)`). If a producer pushed a message right before `a.scheduled = 0` (seeing `a.scheduled == 1` and skipping `active_actors += 1`) and then called `actor.wait_idle()` (or exited `scoped_scheduler`, which calls `scheduler_wait_idle` before `scheduler_stop_and_destroy`), `s.active_actors` transiently hits `0` while that message is still unprocessed—causing `scoped_scheduler` to tear down and free the actor early.


* **The Fix:** Run the `still_has_more` Dekker probe **before** decrementing `s.active_actors`; if `still_has_more` is `true` and `atomic_exchange(&a.scheduled, 1) == 0`, re-push `a` directly onto the shard without ever dropping `s.active_actors` to `0`.


3. **Cross-Pool `actor.forward` Corrupts `Envelope_Pool` Slabs**
* **Where:** `actor_generic_step_proc` and `forward_*` in `actor/actor.odin`, plus `Envelope` in `actor/pool.odin`.


* **The Bug:** `Envelope($Msg)` only tracks a `pool_id: u16` index (`0 ..< MAX_POOLS`), and `actor_generic_step_proc` flushes retired envelopes into `self.pools[env.pool_id]`. If `stage1` and `stage2` were initialized with *different* explicit `[]Envelope_Pool(Msg)` slices (or `stage1` uses explicit pools while `stage2` uses `auto_pools` or `pools = nil`), calling `actor.forward(stage2)` returns `stage1`'s slab memory into `stage2`'s `remote_returns` queue—causing Use-After-Free if `stage1`'s pool is destroyed first, or deadlocking `stage1`'s pool on exhaustion. In addition, `collector_add` silently drops envelopes (`if pid >= MAX_POOLS do return`) if a slice ever has `> 32` pools.


* **The Fix:** Store `origin_pool: ^Envelope_Pool(Msg)` in `Envelope(Msg)` (or assert `stage1.pools` and `stage2.pools` share the same backing pointer) so recycled envelopes always return to the exact `Envelope_Pool` slab that allocated them.


4. **Unchecked `rawptr` Stack Smash on `actor.call` Type Mismatch & Post-Reply `forward` Bug**
* **Where:** `Call_Slot`, `reply_ctx`, and `forward_*` in `actor/actor.odin`, plus `watermark_slab_acquire` in `actor/pool.odin`.


* **The Bug:**
* `actor.call(target, msg, $Res)` erases `&slot` to `rawptr` with no compile-time or runtime check that `$Res` matches the type `T` passed to `actor.reply(val)`. Calling `actor.call(account, Get_Balance{}, i32)` when the handler calls `actor.reply(i64(100))` writes 8 bytes into a 4-byte stack slot, corrupting the caller's stack.


* `reply_ctx` never clears `ENVELOPE_FLAG_CALL` / `ENVELOPE_FLAG_REPLY_PID` or `env.reply_slot` on `ctx.current_env`. If a handler calls `actor.reply(val)` and then `actor.forward(next_actor)`, `next_actor` receives an envelope whose `reply_slot` points to a **popped stack frame**.


* `watermark_slab_acquire` and `watermark_slab_acquire_chain` never reset `slot.flags = ENVELOPE_FLAG_NONE` or `slot.reply_slot = nil` when wrapping around the ring.




* **The Fix:** Store `res_type: typeid` in `Call_Slot` (and `msg_type: typeid` in `Actor_Base`) and assert `res_type == typeid_of(Res)` inside `reply_ctx`; clear `env.flags &~= (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)` and `env.reply_slot = nil` upon replying; and zero `slot.flags` / `slot.reply_slot` in `watermark_slab_acquire`.


5. **Use-After-Free + Double-Free in `examples/library_demo.odin` & Lifecycle Leaks**
* **Where:** `examples/library_demo.odin` (lines 72, 90–91), `actor/actor.odin` (lines 180–217, 299–301), and `actor/spsc.odin` (line 77).


* **The Bug:**
* In `examples/library_demo.odin`, `actor.scheduler_init(&sched, ...)` binds `_tls_active_sched = &sched`, so `aggregator := actor.spawn(...)` registers `aggregator` in `sched.owned_actors`. Line 91 then schedules `defer free(aggregator)`, which runs *before* `defer actor.scheduler_stop_and_destroy(&sched)`—freeing `aggregator` and then immediately dereferencing and freeing it a second time inside `scheduler_stop_and_destroy`.


* In `scheduler_init`, five sequential `make(...) or_return` calls have no `defer if err != nil` cleanup, leaking earlier slices if a later allocation fails. Also, `scheduler_init` overwrites `_tls_active_sched = s` and `scheduler_stop_and_destroy` sets `_tls_active_sched = nil`, breaking any outer `scoped_scheduler` when a nested scheduler exits.


* In `spsc_consume_batch` (`actor/spsc.odin`), passing `max_batch <= 0` converts a negative `int` to `u64(max_batch)` (`~1.84e19`), draining the entire ring instead of `0` items.




* **The Fix:** Replace `defer free(aggregator)` with `defer actor.destroy(aggregator)` in `library_demo.odin`; add `prev_sched: ^Scheduler` restoration + `defer if err != nil` rollback in `scheduler_init`; and guard `if max_batch <= 0 { return 0 }` in `spsc_consume_batch`.


6. **Four AST Parser & Codegen Bugs in `actor/codegen/codegen.odin**`
* **Where:** `extract_pointer_ident`, `parse_source_into_spec`, and `emit_module_code` in `actor/codegen/codegen.odin`.


* **The Bug:**
* **Substring import collision (line 196):** `strings.has_suffix(d.fullpath, "actor\"")` matches unrelated imports ending in `"actor"` such as `import "./refactor"` or `import "math/factor"`, overwriting `mod.actor_import`.


* **Cross-package selector stripping (lines 144–148, 264):** `extract_pointer_ident` strips package prefixes from `^pkg.Type`, so `proc(c: ^http.Client)` false-matches a local `@(actor) Client` struct, and `proc(ctx: ^runtime.Context, self: ^My_Actor)` misidentifies `^runtime.Context` as `^actor.Context`.


* **Grouped parameter loss (lines 270–300):** In `proc(self, peer: ^Bank_Account, amt: i64)`, `fields[0].names` has 2 identifiers (`self` and `peer`), and setting `start_field_idx = 1` silently drops `peer` from both the generated message struct and dispatcher call.


* **Variadic parameters & zero-behavior `-vet` failure (lines 300–314, 367):** Variadic parameters (`items: ..string`) emit invalid struct syntax `items: ..string` instead of `[]string`, and packages with zero matched behaviors still emit `import "../../actor"`, failing `-vet` with an unused import error.







---

## Part 2: Deeper Runtime Hazards in the Proposed OTP Supervision Layer

1. **`vmem.Arena` Default 1 MiB Block Size Blows Up Multi-Tenant RAM (`10,000` Actors = `10 GiB`)**
* **The Hazard:** Calling `vmem.arena_init_growing(&a.base.arena)` with default parameters in Odin's `core:mem/virtual` sets `minimum_block_size` to `DEFAULT_ARENA_GROWING_MINIMUM_BLOCK_SIZE` (**1 MiB**) and immediately maps a 1 MiB block. Across 5,000 organizations $\times$ 3 supervised actors (`15,000` actors), that consumes **15 GiB of virtual/resident memory** before a single message is processed.
* **The Fix:** Do not call `vmem.arena_init_growing` eagerly in `enable_actor_supervision` if `on_reinit == nil`; initialize lazily on first allocation or pass an explicit small block size (`vmem.arena_init_growing(&a.base.arena, 16 * mem.Kilobyte)`), keeping 10,000 lightweight tenant actors in tens of megabytes.


2. **`libc.longjmp` Bypasses Lexical `defer` Blocks (Leaking `xitdb` Write Locks & Mutexes)**
* **The Hazard:** Odin compiles `defer` lexically with zero stack unwinding. When `actor_assertion_failure_proc` executes `libc.longjmp`, **every active `defer` inside the panicking handler and its callees is skipped**. If an actor opened an uncommitted `xitdb` write transaction (`defer xitdb_abort(&tx)`), locked a mutex, or opened a file handle before hitting an out-of-bounds slice check, that lock/handle leaks permanently and deadlocks the tenant or process.
* **The Fix:** Add a small inline rollback stack on `Trap_Frame` (`abort_ hooks: [dynamic; 4]Abort_Hook` registered via `actor.defer_on_fault(ptr, cleanup_proc)`) and invoke all registered rollback hooks in LIFO order inside the `longjmp` fault branch before resetting the actor.


3. **Hardware Faults (`nil` Dereference `SIGSEGV` & Integer `/ 0` `SIGFPE`) Do Not Call `context.assertion_failure_proc**`
* **The Hazard:** Under `-o:speed`, Odin keeps array/slice bounds checks, single-value union assertions, `assert()`, and `panic()` enabled and routes them all through `context.assertion_failure_proc`. However, dereferencing a `nil` pointer (`ptr.field`) or dividing an integer by zero (`x / 0`) emits raw CPU load/divide instructions that raise hardware **`SIGSEGV` / `SIGFPE**`, terminating the binary unless guarded or trapped at the OS signal layer.
* **The Fix:** Document clearly that `assertion_failure_proc` traps Odin software panics/bounds checks (so `-no-bounds-check` must **not** be passed in production builds), and optionally install a `SA_NODEFER` `sigaction` handler on POSIX worker threads (`sigsetjmp`/`siglongjmp`) if you want `nil`-pointer dereferences in pure Odin code trapped too.


4. **Overwriting `@(reinit)` in `actor.supervise` & Single-Supervisor Slot Limitation**
* **The Hazard:** In Turn 2, `spawn_<actor>` on an `@(actor, supervise)` struct installs the generated `<actor>_reinit_hook`. If the caller then attaches that actor to a supervisor via `actor.supervise(sup, a.raw, state)` (where `on_reinit` defaults to `nil`), `enable_actor_supervision` overwrites the generated `@(reinit)` hook with `nil`. Also, storing a single `supervisor: rawptr` on `Actor_Base` prevents external actors (such as an HTTP/WebSocket stream bridge) from monitoring a worker actor via `actor.monitor` alongside its parent supervisor.
* **The Fix:** Preserve any existing `reinit_fn` in `enable_actor_supervision` when `on_reinit == nil`, and add an inline `monitors: [dynamic; 4]^Actor_Base` buffer (guarded by a lightweight spinlock or mutex on `Actor_Base`) so both supervisors and external bridge actors receive `Child_Down` notifications.



Would you like me to generate the complete, consolidated implementation files (`actor/actor.odin`, `actor/pool.odin`, `actor/spsc.odin`, `actor/supervision.odin`, `actor/codegen/codegen.odin`, and `tests/supervision_test.odin`) with all of these fixes applied?



### 1. Concurrency, Memory Safety & Deadlock Bugs in `actor/*.odin`

* **`Watermark_Slab` + `mpsc_pop_dummy` Unsigned `u64` Underflow Deadlock (`actor/pool.odin:261, 283`, `actor/mpsc.odin:29-33`):**
* `mpsc_init` zero-initializes `q.stub`, leaving `q.stub.pool_id == 0`.


* When a consumer pops the first message via `mpsc_pop_dummy`, `&q.stub` is returned as `retired_stub` with `pool_id == 0`, crediting Producer `0`'s `Producer_Watermark` with `+1` extra retirement.


* Once Producer `0` sends `N` messages (`s.cursor == N`) and another producer sends `1` message (retiring Producer `0`'s `N`th slot), `wm[0].retired_count` reaches `N + 1`. In `watermark_slab_acquire`, `s.cursor - s.cached_retired` evaluates `u64(N) - u64(N + 1)` = `0xFFFFFFFFFFFFFFFF`, which satisfies `>= u64(N - 2)` and locks Producer `0` in an infinite spin loop.


* Additionally, calling `watermark_slab_acquire_chain` with `k >= N - 1` makes `(s.cursor + u64(k)) - s.cached_retired >= u64(N - 2)` permanently true even on an empty slab, spinning forever.




* **Type-Unsound Heap Smash & Double-Enqueue in `actor.forward` (`actor/actor.odin:903-969`):**
* `Context.current_env` is an untyped `rawptr`, and `forward_ctx(ctx, target: ^Actor($State, $Msg), updated_msg: Msg)` infers `$Msg` solely from `target` without verifying that `$Msg` matches the caller actor's message type. Calling `actor.forward` from an actor handling an 8-byte message to an actor accepting a 512-byte message compiles cleanly, casts the 32-byte `Envelope` to a 536-byte `^Envelope(Large_Msg)`, overwrites 504 bytes of adjacent `Envelope_Pool` slab memory via `env.msg = updated_msg`, and later recycles that node into the wrong message type's pool.


* None of the `forward_*` procedures assert `!ctx.forwarded` (or clear `ctx.current_env = nil`). Calling `actor.forward` twice in one turn enqueues the exact same intrusive `^Envelope` node into two Vyukov MPSC queues simultaneously, corrupting both mailboxes.




* **`current_context()` Hijacks Application `context.user_ptr` on External Threads (`actor/actor.odin:50-55, 369-374`):**
* On any non-worker thread (`_tls_active_ctx == nil`), `current_context()` falls back to `return cast(^Context)context.user_ptr`.


* If user code, an HTTP framework, or a logger sets `context.user_ptr` to application data and calls `actor.complete_token()` from that thread, `complete_token_ambient` treats `context.user_ptr` as a valid `^actor.Context` and executes an atomic subtract on `ctx.sched.pending_tokens`, causing memory corruption or `SIGSEGV`.




* **Un-Replied `actor.call` Deadlock, Double-Reply Stack Corruption, & Self-Call Hang (`actor/actor.odin:665-695, 1003-1016, 1058-1082`):**
* **Missing `actor.reply`:** If a behavior invoked via `actor.call` returns early without calling `actor.reply` (and without forwarding the envelope), `actor_generic_step_proc` recycles the `ENVELOPE_FLAG_CALL` envelope while `call_sched_pid` hangs forever on `slot.ready == 0`.


* **Double `actor.reply`:** `reply_ctx` does not clear `ctx.current_reply_slot = nil`. Calling `actor.reply` twice in one handler wakes the waiting caller on the first call (allowing `call_sched_pid` to return and reuse its stack frame) and then writes `slot.result = val` directly into the caller's live stack on the second call. Also, `pool_acquire` (`actor/pool.odin:91-135`) never zeroes `env.reply_slot`, leaving stale pointers in `ctx.current_reply_slot` on subsequent turns.


* **Self / Cyclic `actor.call`:** Calling `actor.call(self, ...)` (or `A -> B -> A`) hangs forever without diagnostics because `self.base.scheduled` is already `1` while `self` is mid-turn, so `scheduler_schedule_actor` does not enqueue `self` onto any shard.




* **Data Race on `RunQ_Shard.tail` (`actor/mpsc.odin:76, 82, 102, 139`):**
* `shard_pop_or_steal` reads `intrinsics.atomic_load_explicit(&s.tail, .Relaxed)` **outside** `s.pop_lock`, while another worker holding `s.pop_lock` updates `q.tail = next` inside `mpsc_pop_detached` using plain **non-atomic** stores. Mixing concurrent unsynchronized non-atomic writes and atomic reads on `q.tail` is Undefined Behavior in LLVM's memory model.




* **Unbounded `context.temp_allocator` Growth on Worker Threads (`actor/actor.odin:380-443`):**
* `scheduler_worker_proc` never calls `free_all(context.temp_allocator)` between turns or when parking. Any actor handler using `context.temp_allocator` (e.g., `fmt.tprintf`) leaks into the worker OS thread's temporary arena for the lifetime of the scheduler.




* **`MAX_POOLS :: 32` Silent Envelope Loss & Infinite Spin (`actor/pool.odin:21, 136, 170-172`, `actor/actor.odin:312, 835-844`):**
* If a caller initializes an explicit `[]Envelope_Pool(Msg)` slice with `> 32` pools (e.g., on a 64-core host), `collector_add` silently drops every envelope with `pool_id >= 32` (`if pid >= MAX_POOLS do return`). Once pool `>= 32` exhausts its initial slab, `pool_acquire` (`spin_if_exhausted = true`) spins forever waiting on `remote_returns` that will never arrive.


* For auto-pools on `> 31`-worker machines, workers `32..N` fall off `wid < len(target.pools)` in `send_ctx` and contend on a single `ext_pool.busy` spinlock / heap fallback on every message.




* **Null-Scheduler Dereference, `actor_init` Ordering Trap, Unchecked OOMs, & `wait_*` Hangs (`actor/actor.odin`):**
* `send_ambient_envelope` and `send_ambient_envelope_pid` (`lines 769-782`) omit `assert(s != nil)` before calling `send_envelope` $\rightarrow$ `scheduler_schedule_actor`, dereferencing `nil` at `&s.active_actors` (`line 332`) when no scheduler is bound.


* All four public `actor_init*` procedures (`lines 476-525`) hardcode `s = nil` when calling `actor_init_common` instead of accepting `s: ^Scheduler = nil`; initializing an actor before `scheduler_init` leaves `a.sched = nil` and `a.pools = nil`.


* `spawn_val_auto` and `spawn_sched_val_auto` (`lines 597-623`) omit `#optional_allocator_error` and ignore `new(...)` OOM errors (unlike the other eight `spawn_*` procedures), as do `_create_and_start_scheduler` (`line 232`), `acquire_auto_envelope` (`line 701`), and `pool_acquire` (`actor/pool.odin:130`).


* Calling `wait_idle` / `wait_messages` / `wait_tokens` (`lines 346-365`) before `scheduler_start` (`s.started == false`), or calling `wait_idle` from inside a running actor handler (`s.active_actors >= 1`), busy-spins forever with no assertion.





---

### 2. AST Parser & Code Generator Bugs in `actor/codegen/codegen.odin`

* **Parameter Name Collision With `target` and `reply_to` (`lines 520-558`):**
* Generated `send_*` and `call_*` wrappers hardcode `target: <Actor>_Actor` (and `reply_to: actor.PID(...)`). Any behavior procedure that names one of its own parameters `target` (e.g., `transfer :: proc(self: ^Account, target: Account_Actor, amount: i64)`) or `reply_to` emits duplicate parameter names in the generated signature and fails to compile.




* **`using raw` + `using vtable` Namespace Collision & Broken `<Actor>_PID` Alias (`lines 456-462`):**
* `<Actor>_Actor` embeds both `using raw: ^actor.Actor(...)` and `using vtable: ^<Actor>_VTable`. Any `@(actor)` behavior named `id`, `state`, `next`, `sched`, `mailbox`, `pools`, `allocator`, `scheduled`, `user_ptr`, `receive`, `ref`, or `base` collides with fields promoted from `Actor`/`Actor_Ref`/`Actor_Base` and fails to compile.


* Line 462 emits `<Actor>_PID :: <Actor>_Actor` instead of `actor.PID(<Actor>_Msg)`. It neither erases `State` nor matches `actor.PID(<Actor>_Msg)` in `send_*_pid` / `reply_to` signatures.




* **Type-Inferred Defaults (`x := 1`), Blank `_` Parameters, & Backtick Attributes (`lines 114, 299-311, 422, 479`):**
* For Odin parameters with inferred types (`memo := "cash"`), `field.type` is `nil`; `node_source(src, field.type)` returns `""` and emits invalid struct syntax `memo: ,`.


* Blank parameters (`_: int`) emit `_: int` in the message struct, `m._` in the dispatcher, and `{_ = _}` in callers—all invalid Odin syntax.


* `get_attribute` only trims `"` (`strings.trim(lit.tok.text, "\"")`), leaving backticks intact on raw-string attributes like `@(behavior=`reset`)`.




* **`generate_directory` Parses `*_test.odin` and OS-Specific `#+build` Files (`lines 674-679`):**
* `generate_directory` parses every `.odin` file in the folder except `*_gen.odin`. Any test-only `@(actor)` struct inside `*_test.odin` (or platform-gated `*_windows.odin` / `#+build` file) gets emitted into `actors_gen.odin`, breaking normal `odin build` / `odin run` builds where `*_test.odin` is excluded by the compiler.




* **Aliased `actor` Import Dropped (`line 197`):**
* `mod.actor_import = d.fullpath` discards `d.name.text`. If a file imports the runtime from a differently named directory via an alias (`import actor "../sleipnir_rt"`), `actors_gen.odin` emits `import "../sleipnir_rt"` (binding `sleipnir_rt`) while hardcoding `actor.*` throughout the generated file.





---

### 3. Documentation & Demo Bugs (`README.md` & `benchmarks/README.md`)

* **Non-Compiling Example in `README.md` (`README.md:192` vs `actor/spsc.odin:140-145`):**
* Example 5 calls `actor.sharded_spsc_consume(&mbox, max_per_lane = 64, ctx = &total, ...)`, whereas the actual parameter names in `sharded_spsc_consume` are `quota_per_lane` and `state`.




* **Broken Link & Stale Identifiers in `benchmarks/README.md` (`lines 13, 17, 158, 188`):**
* Links to `../actor/spsc_mailbox.odin` (actual file is `actor/spsc.odin`) and describes `package actor` using non-existent internals (`Chase-Lev` deque instead of `Ticket_Mutex` `RunQ_Shard`, `push_return_chain`, `Actor_Base.process_batch`, and `target.home_worker`).





---

### 4. Two Platform Traps in `setjmp` / `longjmp` Supervision

* **Per-Turn `sigprocmask` Syscall Overhead on macOS / BSD (`libc.setjmp`):**
* On Darwin and BSD targets, `libc.setjmp` saves the thread's signal mask via a kernel `sigprocmask` syscall on every invocation, adding hundreds of nanoseconds to every actor batch turn. Calling `_setjmp` on POSIX—or establishing a single `setjmp` checkpoint once at the top of `scheduler_worker_proc` (with a type-erased `a.recover_proc` on `Actor_Base` to flush `collector`) rather than per `step_proc`—keeps happy-path cost at `0 ns`.


* **Windows x64 SEH Stack-Unwind Crash (`STATUS_BAD_STACK`) With Foreign `libc.setjmp`:**
* On Windows x64, `ucrt`'s `_setjmp` expects compiler-intrinsic lowering that passes the caller's `RSP` frame pointer in `RDX` for Structured Exception Handling (SEH) unwind validation. Calling `libc.setjmp` as an ordinary C FFI proc leaves arbitrary data in `RDX`, causing `longjmp` to abort the process with `0xC0000028` (`STATUS_BAD_STACK`) unless routed through `_setjmp(buf, nil)` (`rdx = 0` disables SEH unwind) or Vectored Exception Handling.


### 1. Additional Bugs in `actor/*.odin` (Core Runtime)

* **Stack Use-After-Free & Unconditional Syscall in `actor.call` / `reply_ctx` (`actor/actor.odin:1006-1011, 1058-1081`):**
* `slot: Call_Slot(Res)` lives on the caller's stack in `call_sched_pid`. In `reply_ctx`, the worker executes `atomic_store_explicit((^u32)(&slot.ready), 1, .Release)` on line 1009 and then calls `sync.futex_signal(&slot.ready)` on line 1010.


* The instant line 1009 becomes visible, a spinning caller on line 1068 (`for atomic_load(&slot.ready) == 0`) can return `slot.result` and pop, reuse, or (on thread exit) unmap its stack frame *before* the worker executes line 1010's `sync.futex_signal(&slot.ready)` on that dangling stack address.


* In addition, `reply_ctx` executes a kernel `futex_signal` syscall on 100% of `actor.call` replies even when the caller never parked.




* **Worker Threads Lose Caller `context` (`Tracking_Allocator` Blindness & `bad_free`) (`actor/actor.odin:252-254, 380-391`):**
* `scheduler_start` creates OS threads via `thread.create(scheduler_worker_proc)` without setting `sched.workers[w].init_context = context`, and `scheduler_worker_proc` never binds `context.allocator = s.allocator`.


* Every actor `receive` handler runs under `runtime.default_context()`, silently discarding the caller's `context.logger`, `context.allocator`, and `context.assertion_failure_proc`. Consequently, `with_leak_check` in `tests/actor_test.odin` cannot detect heap leaks made via `context.allocator` inside actor handlers, and freeing handler-allocated memory on `main`'s `Tracking_Allocator` triggers a `bad_free` mismatch.




* **Pool Exhaustion Deadlocks When `heap_fallback == false` (`actor/actor.odin:799-882`, `actor/pool.odin:59-160`):**
* `pool_init` defaults `heap_fallback` to `false`, and `pool_acquire` / `pool_acquire_chain` default `spin_if_exhausted` to `true`.


* **Batch self-deadlock:** Calling `send_batch_from_pool` (even from an external thread with idle workers) with `len(msgs) > slab_capacity` calls `pool_acquire_chain`, which tries to acquire all `len(msgs)` envelopes *before* pushing any of them to `target.mailbox`—spinning forever on `remote_returns` once the slab runs out.


* **Worker starvation deadlock:** Inside a handler, `send_ctx` calls `send_from_pool` (`spin_if_exhausted = true`). If worker threads emit more than `slab_capacity` messages to unscheduled actors before ending their turns, all workers spin forever inside `pool_acquire` and never step the consumer actors that recycle envelopes.


* **`slab_capacity = 0` hang:** `pool_init` allows `slab_capacity <= 0` (`if slab_capacity > 0` at line 69) while keeping `heap_fallback = false`, causing the very first `pool_acquire` to spin infinitely.




* **Cross-Scheduler Race & Runqueue Hijack in `send_ctx` / `send_ambient` (`actor/actor.odin:835-900`):**
* Unlike `acquire_auto_envelope` (which checks `ctx.sched == s`), `send_ctx` and `send_ambient` never check `ctx.sched == target.sched`. If an actor on Scheduler `s1` sends a message to an actor owned by Scheduler `s2`, Worker `wid` of `s1` mutates `target.pools[wid]`'s non-atomic `local_free` freelist concurrently with Worker `wid` of `s2`, and enqueues `target` onto `s1`'s shards so both schedulers execute `target` concurrently.




* **Use-After-Free & Double-Free When `actor_destroy` Runs After `scheduler_stop_and_destroy` (`actor/actor.odin:286-295, 629-652`):**
* `scheduler_stop_and_destroy` frees every registered actor in `s.owned_actors` via `a.destroy_proc(a)`. If a caller also calls `actor.destroy(a)` *after* `scheduler_stop_and_destroy(&sched)` (e.g., due to `defer` declaration order or struct cleanup), `actor_destroy` reads `a.base.reg_sched` and `a.allocator` from freed heap memory and double-frees `a`.




* **100% Shard `0` Skew on External Sends & `nil` Scheduler Crash on Pre-Init Actors (`actor/actor.odin:312, 446, 673-697`):**
* `scheduler_get_or_create_pools` allocates `max(s.num_shards, s.num_workers) + 1` pools (`ext_idx = len(pools) - 1`). Whenever `num_shards >= num_workers`, `ext_idx == s.num_shards`; `acquire_auto_envelope` returns `hint = ext_idx` for external callers (`shard_hint = -1`), and `u32(ext_idx) % u32(s.num_shards)` evaluates to `0`—routing every external send exclusively to `shards[0]`.


* If an actor is initialized or spawned while `_tls_active_sched == nil`, `a.sched` stays `nil` even after `send_sched_auto` populates `a.pools`; any subsequent ambient `actor.send(a, msg)` or `actor.call(a, msg, Res)` from a thread without `_tls_active_sched` hits `assert(s != nil)`.




* **Self-Referential Struct Move UAF, Ignored `cfg.num_shards`, Spinning `wait_*`, & Dead `target_messages` (`actor/mpsc.odin:29-33`, `actor/actor.odin:169, 188, 346-381`):**
* `mpsc_init` stores interior pointers `q.head = &q.stub` and `q.tail = &q.stub`; moving an initialized `Actor`, `Envelope_Pool`, or `MPSC_Queue` by value (or resizing a `[dynamic]Actor`) leaves `head`/`tail` pointing to old memory.


* `scheduler_init` line 188 (`cfg.num_shards if cfg.num_shards >= s.num_workers else s.num_workers`) silently overrides any explicit `0 < cfg.num_shards < s.num_workers`, contradicting line 128's doc comment.


* `wait_idle`, `wait_tokens`, and `wait_messages` busy-spin at 100% CPU even when `enable_parking = true`, while `complete_token` broadcasts `s.wake_futex` (waking parked workers instead of waiting callers), and `Scheduler.target_messages` (`line 169`) is dead code never read anywhere in `package actor`.


* `spawn_self` (`lines 629-637`) fails `where !intrinsics.type_is_pointer(Msg)` in `spawn_val_auto` if `State.receive` takes a pointer message `msg: ^Payload`, and dereferences `nil` on a worker thread if `initial_state.receive == nil`.





---

### 2. Additional Bugs in `actor/codegen` (`tools/sleipnir_gen`)

* **`@(actor)` DSL Actors Cannot Target Each Other With Async `send_<name>(..., reply_to)` (`actor/codegen/codegen.odin:540-546`):**
* Generated `_reply_actor` and `_reply_pid` procedures require `reply_to: actor.PID(<Return_Type>)`, whereas every `@(actor)` DSL actor's mailbox accepts `actor.PID(<Actor>_Msg)` (a generated tagged union of per-behavior structs). Two `@(actor)` DSL actors can never use async reply forwarding together without spawning a raw non-DSL adapter actor.




* **Broken Fallback `import "actor"`, Cross-File Import Alias Collision, & Substring `-vet` Failure (`actor/codegen/codegen.odin:198-212, 342-368`):**
* If a package contains only `@(actor)` structs and behavior procedures without explicitly importing `actor` in hand-written files (as `bank_account.odin` and `audit_logger.odin` do when standalone), `mod.actor_import` is empty and line 363 emits `import "actor"`, failing to compile without a collection flag.


* `parse_source_into_spec` deduplicates imports across all files in the directory by `existing.alias == alias`. Because Odin `import` aliases are file-scoped, `import t "core:time"` in File A silently suppresses `import t "my/types"` in File B.


* `is_import_referenced` checks `strings.contains(..., fmt.tprintf("%s.", alias))`, false-matching `"os."` inside `my_os.Config` or default string literals `"foo_os.log"` and emitting unused imports that fail `-vet`.




* **File-Scope Privacy, Polymorphic Helpers, `Context` Actor Misrouting, & Name Collisions (`actor/codegen/codegen.odin:87-102, 215-275, 415-425`):**
* File-level `#+private file` tags, `@(actor, private="file")` structs, and `@(private="file")` constants used in default parameter values are referenced from `actors_gen.odin`, failing compilation.


* Any unannotated polymorphic helper procedure taking `self: ^My_Actor` (e.g., `proc(self: ^My_Actor, v: $T)`) is auto-registered as a behavior and emits invalid struct fields (`v: $T`).


* `@(actor) Context :: struct` misroutes 2-pointer behaviors (`first_ptr == "Context"` at line 264) onto the second parameter's type; `to_snake_case` collides `FooBar` and `Foo_Bar` into `foo_bar`; two procedures on the same actor tagged with the same `@(behavior="x")` emit duplicate struct/union/vtable declarations; and `emit_module_code` leaks temporary builders/strings into `context.temp_allocator`.





---

### 3. Bugs in Benchmarks, Examples, CI & Editor Config

* **Use-After-Free via `defer` Ordering in `benchmarks/actor_benchmark.odin` & `examples/library_demo.odin` (`actor_benchmark.odin:1148-1201, 1217-1260, 1301-1339`, `library_demo.odin:169-210, 250-305`):**
* `defer actor.scheduler_stop_and_destroy(&sched)` is registered *before* `defer delete(actors)`, `defer delete(pools)`, and `defer delete(tokens)`, so Odin's LIFO `defer` frees `tokens`, `actors`, and `pools` **while the scheduler's worker threads are still running**.


* In `bench_library_a2a`, `actor.complete_token` fires *inside* `step_proc` on the final hop; `main` immediately wakes from `scheduler_wait_tokens`, reads `actors[i].state.hash_state` concurrently, and frees `tokens` and `actors` while workers are still finishing `step_proc`, unscheduling `a.scheduled = 0`, and probing `a.step_proc(a, &ctx, 0)` on the freed memory.




* **Data Races, OOB Indexing, Hardcoded `libponyrt` Offset, & Broken Build Command in `benchmarks/` (`actor_benchmark.odin:869-875, 1066-1071, 1572, 1717`, `actor_benchmark_c11.c:666-671, 722`, `benchmarks/README.md:251-255`):**
* `ws_worker_proc` (`actor_benchmark.odin:869-872`) clears `a.scheduled = 0` with `.Release` instead of `.Seq_Cst` (Dekker StoreLoad lost-wakeup hang), reads `&a.mailbox.tail.next` after releasing `a.scheduled` (racing with another worker popping `a`), and indexes `&s.shards[wid * 2]` (out-of-bounds if `NUM_WORKERS > 2`).


* `a2a_worker_proc` (`actor_benchmark.odin:1066-1067` and `actor_benchmark_c11.c:666-667`) reads `a.tail` non-atomically *after* storing `a.scheduled = 0`, racing with peer workers stealing and mutating `a.tail`.


* `actor_benchmark.odin:1572, 1656` writes `(^c.size_t)(uintptr(a) + 224)^ = 1` using a hardcoded ABI offset into `pony_actor_t`, and `spawn_self_mode` (`line 1720`) hardcodes Linux-only `"/proc/self/exe"` while ignoring `fork`/`execv` errors.


* The C11 compile command in `benchmarks/README.md:251-255` omits `-I.tmp/ponyc_src/src/libponyrt -I.tmp/ponyc_src/src/common` (failing on `#include "pony.h"`), and `actor_benchmark_c11.c` omits Workloads 2–4 despite its file header and masks checksums with 31-bit `0x7FFFFFFFULL` instead of Odin's 63-bit `0x7FFFFFFFFFFFFFFF`.




* **CI & `ols.json` Custom-Attribute Errors (`.github/workflows/ci.yml:45-46`, `ols.json:1-12`):**
* `.github/workflows/ci.yml` passes `-custom-attribute:actor,behavior` without `no_behavior`, and `ols.json` omits `"checker_args": "-custom-attribute:actor,behavior,no_behavior"`, causing OLS to flag every `@(actor)` and `@(behavior)` attribute as an error.





---

### 4. Additional Bugs in the Proposed OTP Supervision Code (Turns 1–4)

* **Fixed `[dynamic; 16]time.Tick` Silently Disables Crash Escalation Whenever `max_restarts > 16`:**
* Both the prompt's `Supervisor_State` and Turn 1's `Supervisor_State` use `restart_history: [dynamic; 16]time.Tick`. In Odin, `append` on a fixed-capacity `[dynamic; 16]T` stops growing at length `16`. Any supervisor configured with `max_restarts > 16` (e.g., `max_restarts = 20`) never satisfies `len(sup.restart_history) >= sup.max_restarts` and infinite-restarts a crashing child at 100% CPU forever.


* **Single-Pass `.Rest_For_One` / `.One_For_All` Violates OTP Shutdown/Startup Ordering:**
* Turn 1 restarted children in a single forward loop (`idx ..< len(children)`), restarting upstream dependencies (`Xitdb_Owner`) *while* downstream dependents (`Org_Router`, `Bot_Worker`) are still running and sending messages from stale pre-crash state, instead of stopping dependents in reverse order (`len - 1` down to `idx`) before restarting in forward order (`idx` up to `len - 1`).


* **Unchecked `$Msg` Cast & Unsynchronized Map Access in `Dynamic_Supervisor` / `examples/06_otp_supervision.odin`:**
* Turn 3's `get_or_spawn_tenant(ds, org_id, $Msg)` casts `rec.subtree.router` to `^Actor_Ref(Msg)` without verifying `rec.subtree.msg_type == typeid_of(Msg)`, corrupting the router's mailbox on any message-type mismatch. It also mixes atomic and non-atomic accesses on `core:time.Tick`'s internal `_nsec` field, and Turn 4's `examples/06_otp_supervision.odin` reads and mutates `dyn_sup.tenants[101]` / `dyn_sup.tenants[102]` without acquiring `dyn_sup.mu`.
