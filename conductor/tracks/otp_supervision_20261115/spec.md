# Track Specification: Core OTP Supervision Trees, Per-Actor Panic Recovery, Typed Monitors & `try_call`

**Track ID:** `otp_supervision_20261115`
**Prerequisite Tracks:**
- [`runtime_codegen_hardening_20261115`](../runtime_codegen_hardening_20261115/spec.md) (Track 1)
- [`otp_extended_testing_suite_20261115`](../otp_extended_testing_suite_20261115/spec.md) (Track 2 — pre-authored Layer 1–4 Red ATDD harness & specs)

**Primary References:**
- [`OTP Supervision and Panic Recovery in Odin.md`](references/OTP%20Supervision%20and%20Panic%20Recovery%20in%20Odin.md)
- [`OTP carbon review.md`](references/OTP%20carbon%20review.md)
- [`Porting OTP test scenarios.md`](references/Porting%20OTP%20test%20scenarios.md)
- Upstream implementations in `.tmp/reference_repos/{otp,actor-framework,elixir,ractor-supervisor}`

---

## 1. Executive Summary & Architectural Principles

Bring Erlang/OTP fault isolation ("let it crash"), CAF-style typed process monitors and request-response cancellation, and full production supervisor semantics to Sleipnir (`package actor`) **without adding atomic/locking overhead to the hot message-send path**, turning Track 2's Red core & extended supervision specifications (`spec_01`–`spec_16`, `spec_19`–`spec_26`, `spec_28`, `prop_18`) Green on the first pass without intermediate refactoring.

Key architectural commitments:
1. **In-Place Restart with Stale-Generation Rejection (Zero Registry Locks on Hot Path):** Restarting a crashed actor reinitializes its `state` and resets its optional per-actor `vmem.Arena` in place on the existing `Actor($State, $Msg)` memory block (`Actor_Base`), bumping `base.generation += 1` so existing `PID($Msg)` handles stay valid (`O(1)` lock-free delivery) while supervisors deduplicate stale `Child_Down` events by generation.
2. **Low-Overhead Per-Actor Fault Trapping (`_setjmp`/`_longjmp` + `Trap_Frame`):** Wrap actor turn execution inside an `@(no_inline) run_trapped_turn` trampoline using `_setjmp` on POSIX/Darwin (zero `sigprocmask` syscall per turn) and `_setjmp(buf, nil)` on Windows x64, paired with Odin's scoped `context.assertion_failure_proc` so `panic(...)`, `assert(...)`, and bounds checks unwind only the faulting actor without crashing the OS worker thread. Also wrap `child.on_stop` (`child_stop_proc`) hooks in `Trap_Frame` (`spec_20`) so panicking teardown hooks never abort supervisor restarts or shutdowns.
3. **Complete OTP `Supervisor` State Machine Implemented Once (Erlang `supervisor.erl` + `ractor-supervisor`):** Stop affected children in **reverse startup order** (`len - 1` down to `idx`) via fault-contained `on_stop`, reinitialize non-`.Temporary` survivors in **forward startup order** (`idx` up to `len - 1`) via trapped `on_reinit` with sibling rollback on partial cohort reinit failure (`spec_21`), skip & reclaim `.Temporary` bystanders immediately (`spec_22`), guard lifecycle transitions with atomic `SUP_RUNNING`/`SUP_STOPPING`/`SUP_STOPPED` shutdown race checks (`spec_23`), tear down deep trees iteratively via an explicit work stack/queue (`spec_24`), support dynamic child administration (`terminate_child`/`restart_child`/`delete_child`/`count_children`/`max_children`, `spec_25`), and prune an unbounded `[dynamic]time.Tick` intensity window via an injectable clock (`spec_07`, `spec_08`, `spec_26`).
4. **Green Verification Against Track 2 Red ATDD Suite:** Turn Track 2's pre-authored specs (`spec_01`–`spec_16`, `spec_19`–`spec_26`, `spec_28`, `prop_18`) in `tests/supervision_spec_test.odin` Green phase-by-phase with `0` memory leaks under `mem.Tracking_Allocator`.

---

## 2. Detailed Functional & Technical Requirements

### 2.1 Low-Overhead Panic Trapping, `Trap_Frame`, Fault-Contained `on_stop` & `vmem.Arena` Isolation (Specs 14, 15, 16, 20)

1. **Platform `_setjmp`/`_longjmp` FFI (`actor/supervision.odin`):**
   - **Linux / FreeBSD / OpenBSD:** Bind `@(link_name = "_setjmp")` and `@(link_name = "_longjmp")` from `system:c` (`unix_setjmp`, `unix_longjmp`) to avoid `sigprocmask` syscalls on every actor batch turn.
   - **macOS / Darwin:** Bind `@(link_name = "_setjmp")` and `@(link_name = "_longjmp")` (non-signal-mask-saving BSD semantics).
   - **Windows x64:** Bind `@(link_name = "_setjmp") win32_setjmp :: proc "c" (env: ^libc.jmp_buf, frame_address: rawptr) -> c.int` and pass `nil` as `frame_address` (`rdx = 0`) to disable SEH stack unwinding that would otherwise trigger `STATUS_BAD_STACK` (`0xC0000028`).

2. **`Trap_Frame` & `@(no_inline)` Trampoline:**
   - Declare `frame: Trap_Frame = ---` (uninitialized `= ---` skips zeroing ~4 KiB `libc.jmp_buf` on Darwin per turn), explicitly initializing `prev`, `active_actor`, `hook_count = 0`, and `fault`.
   - Maintain thread-local `@(thread_local) _tls_trap_frame: ^Trap_Frame` (saved/restored on push/pop so nested supervisor `on_reinit`/`on_stop` traps and cooperative `actor.call` turns compose cleanly).
   - Isolate `_setjmp` and `a.step_proc` inside an `@(no_inline) run_trapped_turn` helper so LLVM `-o:speed` register allocation in `scheduler_worker_proc` never clobbers loop locals (`processed`, `collector`) across a `longjmp`.
   - **Fault Formatting Without Allocation (`Fault_Info`):** Use fixed inline storage `msg_buf: [128]u8` and `msg_len: int` (plus `kind: Fault_Kind`, `error_code: i32`, `file: string`, `line: i32`) formatted via `fmt.bprintf` inside `actor_assertion_failure_proc`, ensuring OOM or allocator corruption panics never invoke `context.temp_allocator`.

3. **Fault-Contained `on_stop` Hooks (`spec_20`):**
   - Wrap every `child.on_stop` (`child_stop_proc`) invocation inside a `Trap_Frame` during both `supervisor_terminate_range` strategy restarts and `stop_supervisor` / `scheduler_stop_and_destroy` teardown.
   - If `on_stop` raises an Odin panic or calls `actor.fail_turn(...)`, catch the fault, emit a diagnostic/telemetry event (`Supervision_Event.Child_Stop_Fault`), transition the child to `.Stopped`, rewind/destroy its arena safely, and continue stopping/restarting remaining siblings without unwinding out of the supervisor's turn.

4. **Turn Lifecycle Control & LIFO `defer_on_fault` Hooks (`spec_15`):**
   - `exit_turn()`: Aborts the current turn with normal exit (`longjmp` code `2` -> `Exit_Reason = .Normal`), **without** running `defer_on_fault` hooks (`spec_15`).
   - `fail_turn(err_code: i32 = 1)`: Aborts the current turn with abnormal failure (`longjmp` code `3` -> `Exit_Reason = .Error_Return`, `error_code = err_code`), triggering `defer_on_fault` cleanup hooks in **LIFO reverse registration order**.
   - `defer_on_fault(cleanup: proc(rawptr), arg: rawptr)`: Appends to `frame.abort_hooks: [MAX_ABORT_HOOKS]Abort_Hook` (`MAX_ABORT_HOOKS :: 8`) with zero heap allocation; invoked strictly on abnormal termination (`fault_code != 0 && fault_code != 2`), never on normal return or `exit_turn()`.

5. **Per-Actor `vmem.Arena` Memory Isolation & Leak-Free Faults (`spec_14`):**
   - `Reset_Desc` holds type-erased reset callbacks (`init_state_copy: rawptr`, `state_size: int`, `reinit_proc`, `stop_proc`), `has_arena: bool`, `arena: vmem.Arena`, and `arena_alloc: mem.Allocator`.
   - Initialize `vmem.Arena` either lazily on first actor spawn/turn or with a small initial block size (`16 * mem.Kilobyte`) via `vmem.arena_init_growing(&desc.arena, 16 * mem.Kilobyte)` so 10,000+ supervised actors do not commit gigabytes of virtual address space prematurely.
   - Bind `context.allocator = desc.arena_alloc` during the actor turn when `has_arena == true`, and **temporarily rebind `context.allocator = s.allocator` inside `spawn_*`** so child actors spawned from inside a parent handler do not live inside the parent's resettable `vmem.Arena`.
   - On actor reinit, call `vmem.arena_free_all(&desc.arena)` to reclaim all leaked handler allocations in `O(1)` before re-running `on_reinit` (`spec_14`).
   - Inside `actor_generic_step_proc`, pass or flush `Batch_Return_Collector(Msg)` immediately before propagating a fault (`processed = -1`) so already-popped envelopes in the faulted batch are recycled instead of leaked (`spec_14`, `spec_12`).

---

### 2.2 CAF-Style Typed Monitors, Fault-Aware `try_call` & `actor.forward` Delegation (Specs 10–13, 16, 19, 27-monitors, 28)

1. **`Monitor_Ref`, `Down_Event`, Late-Attach & Incoming Edge Cleanup (`spec_10`, `spec_11`, `spec_27`):**
   - Structures:
     - `Exit_Reason :: enum u8 { Normal, Panic, Error_Return, Supervisor_Shutdown, Escalated }`
     - `Monitor_Ref :: struct { id: u64, target_base: ^Actor_Base }`
     - `Down_Event :: struct { ref: Monitor_Ref, pid_raw: rawptr, generation: u32, reason: Exit_Reason, fault: Fault_Info }`
   - Procedures:
     - `monitor(watcher: PID(Down_Event), target: PID($Msg) / ^Actor($State, $Msg)) -> Monitor_Ref`
     - `demonitor(ref: Monitor_Ref)`
     - `current_generation() -> u32` (returns active actor's `generation` for app code/demos)
   - **Late-Attach Guarantee (modeled on `.tmp/reference_repos/actor-framework/libcaf_core/caf/abstract_actor.cpp`):** Under `target.monitors_mu`, check `atomic_load(&target.status) == .Stopped`. If already permanently stopped, snapshot `target.last_exit_reason`, `target.last_fault`, and `target.generation`, **unlock `target.monitors_mu`**, and immediately send `Down_Event` to `watcher` outside the lock (`spec_10`) to prevent deadlock.
   - **Duplicate Monitors & Selective `demonitor`:** Each call to `monitor` assigns a unique monotonic `id` so multiple independent `monitor` calls from the same watcher to the same target produce distinct `Monitor_Ref` values; `demonitor(ref)` removes only the matching `id` (`spec_11`).
   - **Incoming Monitor Edge Cleanup (`spec_27`):** Upon permanent `.Stopped` transition, notify all attached monitors with `Down_Event` and immediately drain/free `target`'s incoming `Monitor_Node` entries rather than holding dead monitor nodes until `scheduler_destroy`.

2. **Fault-Aware `try_call`, Dead/Stale Fast-Fail & `actor.forward` Call Delegation (`spec_12`, `spec_13`, `spec_16`, `spec_19`, `spec_28`):**
   - `Call_Error :: enum u8 { None = 0, Call_Target_Crashed, Deadlock_Detected, Timeout, Target_Stopped, Stale_Generation }`
   - Overloads `try_call(target, msg, $Res) -> (Res, Call_Error)` (alongside existing `actor.call` asserting `.None` or returning `Res` safely).
   - **Dead-Target & Stale-Generation Fast-Fail (`spec_28`):** At the entry of `try_call` (both external and actor-context paths), inspect atomic `target.status` and `pid.generation`:
     - If `target.status == .Stopped`, return immediately (`< 1ms`) with `({}, .Target_Stopped)` without allocating an envelope or parking on `Call_Slot`.
     - If `pid.generation != atomic_load_explicit(&target.generation, .Acquire)`, return immediately with `({}, .Stale_Generation)`.
   - **Crashed Callee Wakeup (`spec_12`):** On actor abnormal termination/fault during a message marked `ENVELOPE_FLAG_CALL`, if `ctx.current_reply_slot != nil` (callee has not replied yet), transition `Call_Slot` (via offset-0 `ready: sync.Futex`) to `Faulted` (`3`) and wake the waiting caller so `try_call` returns `({}, .Call_Target_Crashed)` immediately instead of hanging. Similarly, if a permanently stopped actor drains its remaining mailbox, wake every queued `ENVELOPE_FLAG_CALL` slot with `.Call_Target_Crashed` or `.Target_Stopped`.
   - **Reply-Before-Crash Preservation (`spec_13`):** Because `reply_ctx` clears `ctx.current_reply_slot = nil` as soon as `slot.result` is committed, if the callee calls `actor.reply(val)` and *then* panics before returning from the handler, the caller receives `(val, .None)` cleanly while monitors and supervisors still observe `.Panic`.
   - **`actor.forward` Synchronous Call Delegation & Broken Promises (`spec_19`):** When a router receives an `ENVELOPE_FLAG_CALL` message and calls `actor.forward(worker, msg)`, `forward_*` clears `ctx.current_reply_slot = nil` and call flags on the router's stack `Context` while preserving `env.reply_slot` and `env.flags` on the forwarded `Envelope`. If `worker` replies, it completes the caller's `Call_Slot`; if `worker` crashes or stops before replying, `worker`'s fault/drain handler fails the caller's `Call_Slot` with `.Call_Target_Crashed` / `.Target_Stopped`.
   - **Temp Allocator Preservation (`spec_16`):** Ensure caller `context.temp_allocator` scratch allocations made before calling `try_call` / `call` on a 1-worker scheduler remain valid after the cooperative call returns (`scheduler_step_one_cooperative` must never call `free_all(context.temp_allocator)`).

---

### 2.3 OTP `Supervisor` Engine, Restart Strategies, Cascading Rollback, Child Admin & Escalation (Specs 01–09, 20–26 & Prop 18)

1. **Supervisor Types, Explicit Insertion Order & Synchronous Registration (`spec_01`, `spec_24`):**
   - `Restart_Strategy :: enum u8 { One_For_One, One_For_All, Rest_For_One }`
   - `Restart_Policy :: enum u8 { Permanent, Transient, Temporary }`
   - `Clock_Proc :: #type proc() -> time.Tick`
   - `Supervisor_Config`: `strategy`, `max_restarts` (default `3`), `max_window` (default `5 * time.Second`), `max_children: int` (`<= 0` means unbounded), `clock: Clock_Proc` (nil defaults to `time.tick_now`), `id: u32`.
   - `Supervisor_Msg`: tagged union of `Child_Down`, `Stop_Supervisor`, etc., using `Child_Down` delivered via `monitor` / supervisor death-link so existing `actor.send` stays zero-overhead.
   - Maintain explicit slice insertion order in `sup.children` (never hash-map iteration order) so children always start `[0 ..< len)` and stop `[len - 1 ..>= 0]` (`spec_24`).
   - `supervise(sup, child, policy, on_reinit = nil, on_stop = nil, use_arena = false)` and `supervise_supervisor(parent_sup, child_sup, policy)`:
     - Protected by `children_mu: sync.Mutex` on `Supervisor_State` so spawning thread registration completes before any worker can process a `Child_Down` event (`spec_01`).
     - Enforces `max_children` backpressure (`spec_25`) and supports `use_arena = true` via `vmem.Arena` reset on `Reset_Desc`.

2. **Atomic Lifecycle Race Guard & Iterative Subtree Teardown (`spec_23`, `spec_24`):**
   - Guard supervisor restart and `Child_Down` event handling with an atomic lifecycle state (`SUP_RUNNING`, `SUP_STOPPING`, `SUP_STOPPED`). If `stop_supervisor` or `scheduler_stop_and_destroy` transitions the supervisor to `SUP_STOPPING` while a worker concurrently traps a child panic, `Child_Down` processing observes `SUP_STOPPING` and declines `on_reinit` without UAF or deadlock (`spec_23`).
   - Implement subtree teardown (`stop_supervisor`) iteratively using an explicit work stack/queue (zero recursion proportional to tree depth) so deep (`16+` level) supervision chains tear down cleanly on bounded worker stacks (`spec_24`).

3. **Restart Policy Matrix & `.Temporary` Bystander Reclamation (Erlang/OTP `supervisor.erl` — `spec_01`, `spec_22`):**
   - `.Permanent`: Always restart regardless of `Exit_Reason` (unless explicitly stopped via `supervisor_terminate_child`, which sets `.Terminated_By_Admin`).
   - `.Transient`: Restart on `.Panic`, `.Error_Return`, or `.Escalated`; transition to `.Stopped` and leave stopped on `.Normal` or `.Supervisor_Shutdown`.
   - `.Temporary`: Never restart on any `Exit_Reason`; transition permanently to `.Stopped`. When stopped as a bystander during `.One_For_All` or `.Rest_For_One`, execute `on_stop` under `Trap_Frame`, **skip** forward `on_reinit`, do **not** increment `restart_history` for bystander termination, and immediately reclaim `Reset_Desc` / child arena memory (`spec_22`).

4. **Ordered Restart Strategies & Cascading Sibling `on_reinit` Rollback (`spec_02`, `spec_03`, `spec_04`, `spec_20`, `spec_21`):**
   - **`.One_For_One` (`spec_02`):** Stop `on_stop` (under `Trap_Frame`) and reinitialize only the failed child `children[idx]`. Siblings remain completely untouched (`generation` unchanged).
   - **`.Rest_For_One` (`spec_03`):**
     1. Iterate backward `i := len(children) - 1; i > idx; i -= 1`: transition live siblings to `.Stopping` -> stop (`on_stop` under `Trap_Frame`), bumping generation / waiting safely for any active turn (`in_step`) before mutation.
     2. Iterate forward `i := idx; i < len(children); i += 1`: reinitialize non-`.Temporary` children (`reinit_actor_in_place` -> `vmem.arena_free_all` -> reset initial state -> trapped `on_reinit` -> bump `generation += 1` -> mark `.Running`).
   - **`.One_For_All` (`spec_04`):**
     1. Iterate backward `i := len(children) - 1; i >= 0; i -= 1`: skip `i == idx` and `.Stopped` entries; call `on_stop` under `Trap_Frame` and mark stopped.
     2. Iterate forward `i := 0; i < len(children); i += 1`: reinitialize all non-`.Temporary` children (`generation += 1`).
   - **Cascading Sibling `on_reinit` Rollback (`spec_21`):** If child `C_k` restarts cleanly (`.Running`) and a younger sibling `C_{k+1}` fails `on_reinit` (`false` / panic / `fail_turn`), immediately roll back (stop in reverse order via trapped `on_stop` and rewind arenas to `.Stopped`) all already-restarted peers in the current cohort so no half-initialized cohort remains `.Running`, record the failed restart attempt against `restart_history`, and retry if within `max_restarts` or escalate otherwise.

5. **Generation Deduplication (`spec_05`):**
   - Each `Child_Entry` tracks `expected_gen: u32`. When handling `Child_Down`, compare `down.generation != entry.expected_gen`; if mismatched (e.g. a sibling already triggered `.One_For_All` restart before the second `Child_Down` was dequeued), discard as a stale notification without counting against `max_restarts`.

6. **Faulty `on_reinit` Trap, Unbounded Sliding Intensity Window & Quiet-Period Reset (`spec_06`, `spec_07`, `spec_08`, `spec_26`):**
   - Wrap `reinit_actor_in_place` in a nested `Trap_Frame` inside the supervisor loop (`spec_06`): if `on_reinit` panics or fails, catch safely, roll back any restarted cohort peers (`spec_21`), append timestamp to `restart_history`, prune expired entries, and retry iteratively until recovery succeeds or `len(restart_history) > max_restarts` triggers meltdown.
   - Maintain unbounded `restart_history: [dynamic]time.Tick` allocated on `s.allocator` (`spec_07` — supports `max_restarts > 16`).
   - Prune entries where `time.tick_diff(t, now) > max_window` using the configured `clock()` (`spec_08`, `spec_26` quiet-period multi-burst survival via `Virtual_Clock`).

7. **Dynamic Child Administration (`spec_25`):**
   - Implement `Supervisor_Error`, `Child_Counts`, `Child_Info`, `supervisor_terminate_child`, `supervisor_restart_child`, `supervisor_delete_child`, `supervisor_count_children`, and `supervisor_which_children`:
     - `supervisor_terminate_child`: stops running child (`on_stop` under `Trap_Frame`) and marks non-`.Temporary` specs `.Terminated_By_Admin` (preventing automatic `.Permanent` restart while retaining the spec until `restart_child` or `delete_child`); `.Temporary` specs are removed cleanly.
     - `supervisor_restart_child`: re-initializes `.Stopped` / `.Terminated_By_Admin` specs via `on_reinit` (increments `generation`); returns `.Child_Still_Running` if `.Running`.
     - `supervisor_delete_child`: removes `.Stopped` / `.Terminated_By_Admin` specs; returns `.Child_Still_Running` if `.Running`.
     - Enforce `max_children` limit returning `.Max_Children_Reached` when `active >= max_children`.

8. **Meltdown & Hierarchical Escalation with Subtree Preservation (`spec_09`):**
   - On meltdown:
     - Iteratively stop all live children in reverse startup order with `Exit_Reason = .Supervisor_Shutdown` **while keeping `children` slice entries intact (`clear_roster = false`)**.
     - Transition supervisor itself to `.Stopped` with `last_exit_reason = .Escalated` and emit `Down_Event` to parent supervisor / external watchers.
   - When a `parent_sup` restarts a supervised `child_sup` (`supervise_supervisor`):
     - `child_sup`'s `on_reinit` clears `child_sup.restart_history`, transitions `SUP_RUNNING`, and reinitializes all registered children in `child_sup.children` in forward startup order (`0 ..< len`).

---

## 3. ATDD Specification Matrix (Turned Green in Track 3)

| Spec ID | Title | Key Invariant Verified |
| :--- | :--- | :--- |
| `spec_01` | Restart policy matrix | `.Permanent` / `.Transient` / `.Temporary` x `.Normal` / `.Panic` / `.Error_Return` / `assert(false)` |
| `spec_02` | `.One_For_One` isolation | Siblings keep `state` & `generation`; only faulted worker resets |
| `spec_03` | `.Rest_For_One` startup order | Older siblings untouched; younger stopped in reverse, restarted forward |
| `spec_04` | `.One_For_All` symmetric reset | All live siblings stopped reverse, all restarted forward; `.Temporary` stays stopped |
| `spec_05` | Concurrent crash deduplication | Simultaneous sibling faults under `.One_For_All` trigger 1 restart cycle |
| `spec_06` | Faulty `on_reinit` intensity limit | Panicking `on_reinit` retries cleanly up to `max_restarts`, then escalates |
| `spec_07` | High intensity bound (`max_restarts > 16`) | Unbounded `restart_history` triggers meltdown at `N + 1`, never before |
| `spec_08` | Sliding window expiry via `Virtual_Clock` | Advancing clock past `max_window` prunes stale crashes; no spurious meltdown |
| `spec_09` | Hierarchical supervisor escalation | Child meltdown Escalates to parent, which restarts child subtree & roster |
| `spec_10` | CAF monitor + late attach | Live monitor fires on exit; monitor on already-`.Stopped` target fires immediately |
| `spec_11` | Duplicate monitor & selective `demonitor` | Independent `Monitor_Ref`s; `demonitor` cancels only matching ref |
| `spec_12` | `try_call` wakes immediately on callee panic | Pending synchronous caller receives `.Call_Target_Crashed` instead of hanging |
| `spec_13` | `reply` followed by late panic in same turn | Caller receives `(res, .None)` while supervisor/monitor still sees `.Panic` |
| `spec_14` | `vmem.Arena` heap reclamation on restart | Repeated handler leaks inside arena are reclaimed to `0` leaked bytes on restart |
| `spec_15` | `exit_turn` vs `fail_turn` & LIFO `defer_on_fault` | Normal exit skips abort hooks; abnormal exit runs hooks in strict reverse order |
| `spec_16` | 1-worker cooperative `try_call` + temp scratch | Preserves caller `temp_allocator` scratch when target crashes cooperatively |
| `spec_19` | `actor.forward` call delegation & broken promise | Delegated reply completes caller `Call_Slot`; worker crash yields `.Call_Target_Crashed` |
| `spec_20` | Fault-contained `on_stop` hook | Panicking / `fail_turn` `on_stop` does not abort restart or supervisor shutdown |
| `spec_21` | Cascading sibling `on_reinit` rollback | Younger sibling `on_reinit` failure stops already-restarted older siblings cleanly |
| `spec_22` (sup) | `.Temporary` bystander non-restart | `.Temporary` bystanders stop, skip restart, and release arenas/specs cleanly |
| `spec_23` | Shutdown vs concurrent child crash race | Atomic `SUP_STOPPING` guard prevents straggler restarts or UAF during teardown |
| `spec_24` | Non-alphabetical order & deep iterative teardown | Insertion-order stop/reinit traces; `16`-level deep & `256`-child scaling without overflow |
| `spec_25` (sup) | Dynamic child admin & `max_children` | `terminate_child`, `restart_child`, `delete_child`, `count_children`, `max_children` limit |
| `spec_26` | Quiet-period intensity reset | Multi-burst crashes separated by quiet windows survive; single burst escalates |
| `spec_28` | `try_call` dead/stale fast-fail & multi-call fan-out | Immediate `.Target_Stopped` / `.Stale_Generation` return; partial fan-out crash handling |
| `prop_18` | Randomized fault-storm chaos invariant | 50+ random sends/crashes/restarts across workers, schedulers; `0` leaks |

---

## 4. Acceptance Criteria

1. Track 2's pre-authored core and supervision specifications (`spec_01`–`spec_16`, `spec_19`–`spec_26`, `spec_28`, `prop_18`) pass (Green) in both default debug and `-o:speed` modes with `0` failures and `0` memory leaks under `mem.Tracking_Allocator`.
2. `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors` and `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors` pass with zero warnings.
3. No file in `conductor/tracks/otp_supervision_20261115/references/` is modified or deleted.
