# OTP Supervision and Panic Recovery in Odin

https://gemini.google.com/app/01d2b0ffd7c6c6d0
User prompt: Adding OTP-style supervision on top of an existing Odin actor library is not complicated at all—it is roughly 300 to 500 lines of Odin—
and because the heavy sandbox compute is already offloaded to Antigravity VMs, a single Odin server can easily run thousands of concurrent
organizations on one machine. The reason people previously leaned on OS-process-per-tenant in native languages is fear of a crash taking down
the whole binary, or memory leaks accumulating when an actor dies mid-flight. In Odin, you can solve both inside a single process using
vmem.Arena per actor, xitdb moments, and context.assertion_failure_proc. Why "Let It Crash" Actually Maps Cleanly to Odin In Erlang/Elixir, "let it
crash" works because each process has an isolated heap that gets garbage-collected in one shot when the process dies. In Odin, you get the exact
same guarantee—arguably faster—by giving each stateful actor its own vmem.Arena (for living state) plus context.temp_allocator (wiped after every
single message turn): 1. When an actor processes a message, it commits any durable state or telemetry changes in an atomic xitdb transaction. 2. 3.
If the actor fails mid-turn (an Antigravity stream drops, a JSON payload is malformed, or an invariant fails), the supervisor simply calls
vmem.arena_free_all(&actor.arena) in $O(1)$ time, resets the actor's state struct to {} (rehydrating from the last committed xitdb moment), and
increments the restart counter. Zero leaked heap objects, zero half-mutated memory. 4. The 4 Primitives to Add to Your Odin Actors Lib To turn an
Odin actor library into a full OTP-style supervision tree, you only need four pieces: 1. Generational Handles ( hm.Handle32) + Links & Monitors If you
already use core:container/handle_map (hm.Dynamic_Handle_Map) for actor lookup, your actor IDs are already generational (like Erlang PIDs)—
meaning a dead actor's handle never accidentally resolves to a newly spawned actor in the same slot. - Links & Monitors: Add two inline buffers to
your actor header: - - links: [dynamic; 8]Actor_Handle (bidirectional crash propagation) - - monitors: [dynamic; 8]Actor_Handle (unidirectional {Down,
handle, reason} notification to supervisors) - - Named Registry (Registry): A global sync.RW_Mutex-protected map from a domain key
(Org_Channel_Key{org_id, channel_id}) to the current Actor_Handle. When a supervisor restarts a crashed child, it rebinds the new handle in the
registry so callers don't hold stale handles. - 2. Child_Spec and Restart Intensity Ring Buffers An OTP supervisor is just a regular actor whose state
holds an array of Child_Spec definitions and a sliding window of restart timestamps so a broken loop backs off instead of pegging a CPU core at
100%: Code snippet package otp import hm "core:container/handle_map" import vmem "core:mem/virtual" import "core:time" Actor_Handle :: distinct
hm.Handle32 Restart_Strategy :: enum u8 { One_For_One, // Only restart the crashed child One_For_All, // Restart all children in this supervisor
Rest_For_One, // Restart the crashed child and any children started after it } Restart_Policy :: enum u8 { Permanent, // Always restart (e.g., Org
Router, Telemetry Flusher) Transient, // Restart only on abnormal Exit_Reason (e.g., active Bot Turn worker) Temporary, // Never restart }
Exit_Reason :: enum u8 { Normal, Shutdown, Stream_Timeout, Invalid_State, Panic_Trapped, } Child_Spec :: struct { id: u64, // e.g., org_id or bot_id
policy: Restart_Policy, init_proc: proc(arena: ^vmem.Arena, id: u64) -> (rawptr, Exit_Reason), step_proc: proc(state: rawptr, msg: rawptr) ->
Exit_Reason, current: Actor_Handle, } Supervisor_State :: struct { strategy: Restart_Strategy, max_restarts: int, // e.g., 5 restarts within_window:
time.Duration, // e.g., 10 * time.Second restart_history: [dynamic; 16]time.Tick, children: [dynamic]Child_Spec, } should_escalate_crash :: proc(sup:
^Supervisor_State) -> bool { now := time.tick_now() cutoff := time.tick_add(now, -sup.within_window) // Prune timestamps older than the sliding
window write_idx := 0 for tick in sup.restart_history { if time.tick_diff(cutoff, tick) <= 0 { sup.restart_history[write_idx] = tick write_idx += 1 } }
resize(&sup.restart_history, write_idx) if len(sup.restart_history) >= sup.max_restarts { return true // Intensity exceeded: kill supervisor & escalate up
the tree } append(&sup.restart_history, now) return false } 3. Catching Odin Panics & Bounds Checks Per-Actor (context.assertion_failure_proc) The
one real difference between BEAM and Odin is what happens on an accidental out-of-bounds slice index (items[i]), nil pointer check, or assert(). By
default, Odin aborts the entire OS process. However, Odin routes all bounds checks, slice checks, panic(), and assert() calls through
context.assertion_failure_proc. On your actor worker threads, you can override context.assertion_failure_proc alongside libc.setjmp / libc.longjmp at
the actor message dispatch boundary: - If an actor's step_proc hits an out-of-bounds slice access or panic(), your custom assertion_failure_proc
records the #caller_location into your Odin telemetry library and longjmps back to the worker thread's dispatch loop with
Exit_Reason.Panic_Trapped. - - The worker thread wipes that actor's vmem.Arena, frees its context.temp_allocator, and sends {Down, actor_handle,
.Panic_Trapped} to its supervisor actor—without taking down the Odin server or affecting any other organization. - 4. Dynamic Supervision Tree for
Multi-Tenant SaaS With those primitives in place, your entire SaaS runs inside one Odin binary structured just like an OTP application tree: [Root
Supervisor (.One_For_One)] │ ┌─────────────────────────┼─────────────────────────┐ ▼ ▼ ▼ [Telemetry & Tigris]
[HTTP/WS Bridge to] [Org DynamicSupervisor] [Sync Supervisor] [Main Elixir App] (.One_For_One, keyed by org_id) │
┌───────────────────────┴───────────────────────┐ ▼ ▼ [Org_101 Supervisor (.Rest_For_One)] [Org_102 Supervisor] │
┌───────────────────────┼───────────────────────┐ ▼ ▼ ▼ [Org_101 xitdb Owner] [Org_101 "Jev" Router] [Org_101 Bot
Pool] Notice how .Rest_For_One on the Org_101 Supervisor pays off: if the xitdb Owner actor for Org_101 ever crashes and restarts,
.Rest_For_One automatically restarts Org_101's Router and Bot Pool actors so they attach to the freshly reopened xitdb handle, while Org_102
doesn't even notice. When Single-Server Odin OTP Wins (and the Only Edge Case to Watch) Because all untrusted LLM-generated Python/Bash
code runs remotely inside Antigravity VMs (never inside your Odin process), you do not need OS-process isolation for security. Running one Odin
server with in-process OTP supervision gives you: - Zero IPC orchestration overhead: One binary to deploy, monitor, and connect to your main Elixir
product. - - Massive density: Thousands of idle or active organization supervision trees take only megabytes of RAM on a single box, and you can
still put a 15-minute idle timer on each Org_<id> Supervisor to flush its xitdb to Tigris and shut down its subtree when an org goes quiet. - - The only
rule you must enforce: Any C library you call via FFI inside that single Odin binary (xitdb C bindings, TLS/HTTP clients) must not corrupt global
process memory. As long as your actors keep their state inside their own vmem.Arena and xitdb transactions, a single Odin server with a 400-line
OTP supervisor layer will be rock solid. - How does your current Odin actor library schedule actors across threads right now (e.g., a work-stealing
thread pool, pinned OS threads per actor, or an M:N queue)? We can tailor the supervisor and panic-recovery hook directly to that scheduler model.
the above text didn't know about my code base, so i would like you to help me with this topic and i included the code base this time
Response: Inspecting Sleipnir (`package actor`) shows that your runtime is already a lock-free Pony-style `M:N` work-stealing scheduler with
intrusive Vyukov MPSC mailboxes (`MPSC_Queue(Envelope(Msg))`), `snmalloc`-inspired batched pool reclamation (`Envelope_Pool(Msg)` +
`Batch_Return_Collector(Msg)`), cooperative `actor.call` stepping, and an AST code generator (`tools/sleipnir_gen`).
Tailoring OTP supervision directly to Sleipnir's internals reveals **four critical interactions** with `actor/actor.odin` and `actor/pool.odin` that a
generic actor wrapper would miss—and one major architectural advantage you get for free because of how `Actor_Ref($Msg)` is laid out.

## 1. Four Sleipnir-Specific Architectural Realities


### A. setjmp Must Live Inside actor_generic_step_proc, Not scheduler_worker_proc

In `actor/actor.odin`, `actor_generic_step_proc` pops up to `quota` (default 64) detached envelopes from `self.mailbox` and accumulates
them on the stack inside `collector: Batch_Return_Collector(Msg)` before calling `collector_flush(&collector, self.pools,`
`-1)` at the end of the turn:

> **Code:** [01_current_step_loop.odin](01_current_step_loop.odin) (17 lines)

If an actor panics on message #10 of a 64-message batch and `longjmp` unwinds all the way out to `scheduler_worker_proc`, **messages #0–#9**
**sitting in** `collector` **plus message #10 (**`ctx.current_env`**) are never returned to** `self.pools`, permanently leaking 10 slab slots from

Envelope_Pool(Msg).

By placing the `setjmp` boundary **inside** `actor_generic_step_proc`, the fault path can:
1. Recycle `ctx.current_env` into `collector` (if `!ctx.forwarded`).
2. Call `collector_flush(&collector, self.pools, ctx.worker_id)` so zero `Envelope_Pool(Msg)` slots leak.
3. Wipe `free_all(context.temp_allocator)` and return `(processed, false)` so `scheduler_worker_proc` cleanly unschedules the
faulted actor.

### B. Cooperative actor.call Can Nest step_proc Frames on the Same Worker Thread

In `call_sched_pid` (`actor/actor.odin:1058-1080`), when an actor on a worker thread calls `actor.call(target, msg, Res)` (or
`account->get_balance()` in your `@(actor)` DSL), the waiting worker calls `scheduler_step_one_cooperative(s, caller_ctx)` while
spinning on `slot.ready`.
That creates two requirements for panic recovery:
1. **Stack-linked** `Trap_Frame` **instead of a single flat** `jmp_buf` **on** `Context`**:** Because Worker 0 can be mid-turn inside Actor A while
cooperatively stepping Actor B inside `scheduler_step_one_cooperative`, each `step_proc` invocation must push a stack-allocated
`Trap_Frame` onto a thread-local `_tls_trap_frame` pointer and pop it on exit.
2. **Unblocking** `Call_Slot` **on Callee Crash:** When `env.flags & ENVELOPE_FLAG_CALL != 0`, the caller is spinning on `slot:`
`Call_Slot(Res)` on another stack frame. If the callee actor crashes before calling `actor.reply`, the fault handler must mark `slot.ready`
`= CALL_STATUS_FAILED` and call `sync.futex_signal(&slot.ready)` so the caller wakes up immediately instead of deadlocking.

### C. In-Place Restart Keeps PID($Msg) and <Actor>_Actor Handles Valid (Zero Registry Lock!)

In Erlang, a process crash allocates a new `PID` because the mailbox and heap are fused together. In Sleipnir, `Actor($State, $Msg)` embeds
`Actor_Ref($Msg)` at offset 0 (which owns `mailbox: MPSC_Queue(Envelope(Msg))` backed by external `Envelope_Pool(Msg)` slabs) and
stores `state: State` separately.
Because message envelopes live in `self.pools` rather than the actor's state arena:
**In-place restart (**`vmem.arena_free_all(&a.base.arena)` **+** `a.reinit_proc(a)`**):** A supervisor can wipe a crashed child's
`vmem.Arena` in O(1) time and reinitialize `a.state` **at the exact same** `^Actor(State, Msg)` **memory address**.
Every existing `actor.PID(Msg)` and generated `<Actor>_Actor` struct (`Bank_Account_Actor`, `Audit_Logger_Actor`) held by other
actors remains 100% valid across restarts—**eliminating the need for a global** `sync.RW_Mutex` **registry lookup on the hot message-send**
**path**.
**Optional Generational PID Invalidation in Free Padding:** Right now in `actor/actor.odin`, `Actor_Base` is `#align(64)` and uses 48
bytes (`next`, `scheduled`, `id`, `user_ptr`, `step_proc`, `destroy_proc`, `reg_sched`), leaving **16 bytes of unused padding** in Cache Line 0.
Placing `status: u32` and `generation: u32` in those 8 bytes lets you support both **in-place restarts** (for permanent services) and
**generational handle invalidation** (when an actor is permanently terminated).

### D. Preventing the Dekker Re-Check from Rescheduling a Faulted Actor

At the end of `scheduler_worker_proc` (`actor/actor.odin:429-437`), when `has_more == false`, the worker sets `a.scheduled = 0`
and immediately re-probes `_, still_has_more := a.step_proc(a, &ctx, 0)`. If `a.base.status` is set to `ACTOR_STATUS_FAULTED`
before returning from `step_proc`, the `quota == 0` probe returns `(0, false)` and `scheduler_schedule_actor` refuses to re-queue `a` until its
supervisor finishes resetting its state/arena and transitions `a.base.status` back to `ACTOR_STATUS_RUNNABLE`.

## 2. Diffs to actor/actor.odin & actor/pool.odin


### Step 1: Extend Actor_Base & Call_Slot in actor/actor.odin

We use the existing 16-byte tail padding in `Actor_Base` for `status` and `generation`, and add lifecycle hooks + per-actor `vmem.Arena` support:

> **Code:** [02_actor_base_layout.odin](02_actor_base_layout.odin) (42 lines)

And in `scheduler_schedule_actor` (`actor/actor.odin:329`), guard scheduling with `status == ACTOR_STATUS_RUNNABLE`:

> **Code:** [03_scheduler_schedule_actor.odin](03_scheduler_schedule_actor.odin) (23 lines)


## 3. Drop-In Supervision & Panic Recovery (actor/supervision.odin)

Create `actor/supervision.odin` in `package actor`. It implements:
1. **Stack-linked** `Trap_Frame` **(**`setjmp` **/** `longjmp` **+** `context.assertion_failure_proc`**)** that safely supports nested cooperative
`actor.call` frames on the same worker thread.
2. **Fault-tolerant** `actor_generic_step_proc` that flushes `Batch_Return_Collector(Msg)`, wakes any blocked `actor.call`
`Call_Slot`, wipes `context.temp_allocator`, and notifies the actor's supervisor.
3. **OTP** `Supervisor` **Actor** (`.One_For_One`, `.One_For_All`, `.Rest_For_One`, sliding-window restart intensity, and in-place `vmem.Arena` +
state reset).

> **Code:** [04_supervision.odin](04_supervision.odin) (442 lines)


## 4. Integrating Supervision with tools/sleipnir_gen (actor/codegen/codegen.odin)

Because your generated `<Actor>_Actor` wrapper embeds `using raw: ^actor.Actor(Bank_Account, Bank_Account_Msg)`:
1. `supervise(sup, account.raw, Bank_Account{owner = "Org_101", balance = 500})` works out of the box with zero changes
to `codegen.odin`!
2. Because `supervise` resets `account.raw` **in-place**, any actor holding `account: Bank_Account_Actor` (such as `Audit_Logger` or your
`Org_101` Router) can keep calling `account->deposit(250)` and `account->get_balance()` after a panic and restart without ever
needing to re-bind `account`!
3. If you want `tools/sleipnir_gen` to automatically wire up `on_reinit` (for `xitdb` rehydration on restart), you can check in
`parse_source_into_spec` (`actor/codegen/codegen.odin:225`) for a procedure marked `@(reinit)` taking `(self: ^Actor_Type,`

alloc: mem.Allocator) and emit enable_actor_supervision(a, initial_state, <reinit_proc>) inside
spawn_<actor>_auto and spawn_<actor>_sched.


## 5. 4-Layer ATDD Specification (tests/supervision_test.odin)

Following the exact 4-layer ATDD architecture in `tests/actor_test.odin` (`with_leak_check`, `Runtime_Spec`, Layer 1 specifications, Layer 2
domain verbs with `loc := #caller_location`, and Layer 3 protocol drivers), here is a complete test suite verifying:
1. Out-of-bounds slice panics are trapped per-actor without crashing the binary or leaking `Envelope_Pool` slots mid-batch.
2. Per-actor `vmem.Arena` allocations made during the failing turn are wiped in O(1) time while rehydrating from the last committed state
snapshot.
3. `.Rest_For_One` restarts downstream dependent actors (`xitdb_owner` → `org_router`) when an upstream dependency crashes.

> **Code:** [05_supervision_test.odin](05_supervision_test.odin) (175 lines)

Would you like to extend `tools/sleipnir_gen` (`actor/codegen/codegen.odin`) next so `@(actor)` structs support a declarative `@(reinit)`
or `@(supervise)` attribute in generated `spawn_<actor>` constructors?
For a deeper look at how Odin's implicit `context` threads custom allocators and assertion handlers through procedures like
`actor_generic_step_proc`, watch Context and Allocators in Odin.
User prompt: Would you like to extend tools/sleipnir_gen (actor/codegen/codegen.odin) next so @(actor) structs support a declarative @(reinit) or
@(supervise) attribute in generated spawn_<actor> constructors? ye
Response: Because `.github/workflows/ci.yml` runs `git diff --exit-code`
`examples/05_ast_metaprogramming/actors_gen.odin`, gating the generated supervision and rehydration bindings on `@(supervise)` or
`@(reinit)` keeps existing non-supervised `@(actor)` codegen byte-for-byte identical while unlocking full OTP supervision for annotated actors.

## 1. What You Write With @(supervise) & @(reinit)

You can annotate any `@(actor)` struct with `@(supervise)` (or `@(supervise="Permanent")`, `@(supervise="Transient")`,
`@(supervise="Temporary")`) and mark any state-rehydration procedure with `@(reinit)`:

> **Code:** [06_banking_reinit_example.odin](06_banking_reinit_example.odin) (22 lines)


### What sleipnir_gen Generates for Tenant_Ledger:

1. **Excludes** `rehydrate_ledger` **from** `Tenant_Ledger_Msg`: `@(reinit)` procedures are never turned into message structs or
`send_rehydrate_ledger` callers.
2. **Emits** `<actor>_reinit_hook`: Adapts either 1-arg `proc(self: ^T)` or 2-arg `proc(self: ^T, alloc: actor.Allocator)` to
`actor.enable_actor_supervision` (aliasing `Allocator :: mem.Allocator` in `package actor` so `actors_gen.odin` never fails if
the target package doesn't explicitly import `"core:mem"`).
3. **Wires** `actor.enable_actor_supervision` **into** `spawn_<actor>_auto` **and** `spawn_<actor>_sched`: Every spawned instance
automatically gets its `vmem.Arena` and `@(reinit)` hook initialized.
4. **Adds** `spawn_<actor>_sup` **to the** `spawn_<actor>` **overload group**: Because `^actor.Actor(actor.Supervisor_State,`
`actor.Supervisor_Msg)`, `^actor.Scheduler`, and `Tenant_Ledger` are distinct first-parameter types, you can spawn directly under a
supervisor with:

ledger := spawn_tenant_ledger(sup, Tenant_Ledger{org_id = 101, balance = 500})


## 2. Changes to actor/codegen/codegen.odin

In `actor/supervision.odin`, add one type alias so generated files can reference `actor.Allocator` without requiring `import "core:mem"`:

> **Code:** [07_supervision_allocator_alias.odin](07_supervision_allocator_alias.odin) (2 lines)

Then apply the following additions to `actor/codegen/codegen.odin`:

> **Code:** [08_codegen_reinit_spec.odin](08_codegen_reinit_spec.odin) (14 lines)


### Parsing @(supervise) and @(reinit) in parse_source_into_spec

Update `parse_source_into_spec` (`actor/codegen/codegen.odin:171-320`) to accept `pending_reinits: ^[dynamic]Reinit_Spec`,
inspect `@(supervise)` on `@(actor)` structs, and capture `@(reinit)` procedures before the behavior matcher runs:

> **Code:** [09_codegen_parse_source.odin](09_codegen_parse_source.odin) (63 lines)


### Binding pending_reinits in finalize_module_spec

Update `finalize_module_spec` (`actor/codegen/codegen.odin:324-339`) to attach each `Reinit_Spec` to its `Actor_Spec`:

> **Code:** [10_codegen_finalize_module.odin](10_codegen_finalize_module.odin) (32 lines)


### Emitting <actor>_reinit_hook & spawn_<actor>_sup in emit_module_code

Replace section 5 of `emit_module_code` (`actor/codegen/codegen.odin:564-605`) with:

> **Code:** [11_codegen_emit_module.odin](11_codegen_emit_module.odin) (65 lines)

And in `generate_from_sources` and `generate_directory` (`actor/codegen/codegen.odin:642-714`), pass a `pending_reinits`
dynamic array alongside `pending`:

> **Code:** [12_codegen_pending_reinits.odin](12_codegen_pending_reinits.odin) (6 lines)


## 3. Codegen Specification Test (tests/codegen_test.odin)

Add `test_04_ast_codegen_supervise_and_reinit_attributes` to `tests/codegen_test.odin` to verify that
`@(supervise="Transient")` and `@(reinit)` across both 1-arg and 2-arg signatures generate the expected supervision hooks and never leak
`@(reinit)` procedures into the actor's message union:

> **Code:** [13_codegen_supervise_reinit_test.odin](13_codegen_supervise_reinit_test.odin) (57 lines)

When compiling packages that use `@(supervise)` or `@(reinit)`, include them in the `-custom-attribute` compiler flag:

> **Code:** [14_run_tests.sh](14_run_tests.sh) (2 lines)

Would you like to add a `Dynamic_Supervisor` (keyed by `org_id: u64` with idle-timeout eviction for multi-tenant `Org_<id>` trees) or a named
registry next?
User prompt: yes
Response: Adding a concurrent `Registry($Key)` and a multi-tenant `Dynamic_Supervisor` requires solving one subtle lifecycle detail in
`actor/actor.odin`: when an idle `Org_<id>` supervision tree is evicted after its idle timeout, its actors must be unregistered from
`Scheduler.owned_actors` and have their `destroy_proc` called (freeing both their `vmem.Arena` and heap struct) so
`Scheduler.owned_actors` never accumulates dead pointers in a long-running server.

## 1. Safe Dynamic Teardown (actor_base_destroy)

In `actor/actor.odin`, `register_spawned_actor` appends every spawned `&a.base` to `s.owned_actors` so
`scheduler_stop_and_destroy` can clean up remaining actors on exit. When a `Dynamic_Supervisor` evicts an idle tenant at runtime,
`actor_base_destroy` safely unschedules, unregisters, and frees any type-erased `^Actor_Base` (including its `vmem.Arena` and `Reset_Desc`):

> **Code:** [15_actor_base_destroy.odin](15_actor_base_destroy.odin) (30 lines)

Also update `actor_destroy` in `actor/actor.odin:631-651` to call `a.base.destroy_proc(&a.base)` if present (falling back to `free(a,`
`a.allocator)`), ensuring manual `actor.destroy(a)` calls also free any attached `vmem.Arena`:

> **Code:** [16_actor_destroy.odin](16_actor_destroy.odin) (26 lines)


## 2. Concurrent Named Registry (Registry($Key))

Add this `sync.RW_Mutex`-protected registry to `actor/supervision.odin`. Because Sleipnir restarts supervised actors **in-place**, callers only hit
the `RW_Mutex` when resolving a domain key (e.g. `Org_Channel_Key{org_id, channel_id}`) or when a `Dynamic_Supervisor` spawns/evicts
a tenant:

> **Code:** [17_registry.odin](17_registry.odin) (63 lines)


## 3. Multi-Tenant Dynamic_Supervisor (On-Demand Spawn + Idle Eviction)

Add `Dynamic_Supervisor` to `actor/supervision.odin`. It manages an arbitrary number of dynamically spawned `Org_<id>` subtrees, tracks
last-activity timestamps per tenant, runs an `on_evict` hook (e.g. to flush `xitdb` to Tigris) when a tenant sits idle past `idle_timeout`, and tears
down the entire `Org_<id>` subtree with zero memory leaks:

> **Code:** [18_dynamic_supervisor.odin](18_dynamic_supervisor.odin) (154 lines)

Also add `registry_init` / `dynamic_supervisor_init` and `registry_destroy` / `dynamic_supervisor_destroy` to the `init` and
`destroy` procedure overload groups in `actor/doc.odin` so `actor.init(&reg)` and `actor.destroy(&ds)` work seamlessly.

## 4. 4-Layer ATDD Specification (tests/supervision_test.odin)


Add test_22_dynamic_supervisor_on_demand_spawn_registry_and_idle_eviction to tests/supervision_test.odin to verify

the complete multi-tenant lifecycle under `with_leak_check` (`mem.Tracking_Allocator`):
1. Spawning `Org_101` and `Org_102` on demand with `.Rest_For_One` per-org supervisors (`xitdb_owner` + `org_router`).
2. Routing messages via `Registry(u64)` and isolating a panic in `Org_101` so `Org_102` is completely unaffected.
3. Evicting `Org_101` after its idle timeout expires, verifying the `on_evict` callback flushes `Org_101`, removing it from `Registry(u64)` while
`Org_102` remains active, and confirming **zero leaked bytes** on exit.

> **Code:** [19_test_22_dynamic_supervisor.odin](19_test_22_dynamic_supervisor.odin) (27 lines)

And add the corresponding Layer 2 & Layer 3 helpers to `tests/supervision_test.odin`:

> **Code:** [20_supervision_world_dyn_sup.odin](20_supervision_world_dyn_sup.odin) (114 lines)

Would you like a runnable `examples/06_otp_supervision.odin` demonstrating this full multi-tenant supervision tree (`Dynamic_Supervisor`
→ `Org_<id> .Rest_For_One` → `xitdb` owner + router + worker pool) end to end?
User prompt: yes
Response: Create `examples/06_otp_supervision.odin` to run the full multi-tenant OTP supervision tree (`Dynamic_Supervisor` → per-org
`.Rest_For_One` supervisor → `Xitdb_Owner` + `Org_Router` + `Bot_Worker`) with per-actor `vmem.Arena` wipe, `xitdb` moment rehydration, and
idle tenant eviction:

> **Code:** [21_06_otp_supervision.odin](21_06_otp_supervision.odin) (295 lines)


### Add Example 06 to .github/workflows/ci.yml

Add the new example and custom attributes to `.github/workflows/ci.yml`:

> **Code:** [22_ci_workflow.yml](22_ci_workflow.yml) (8 lines)
