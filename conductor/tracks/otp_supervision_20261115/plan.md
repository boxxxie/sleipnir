# Implementation Plan: Core OTP Supervision Trees, Per-Actor Panic Recovery, Typed Monitors & `try_call`

This plan implements Track 3 in 4 Green-turning phases against the Red-First Layer 1–4 ATDD harness established in [`otp_extended_testing_suite_20261115`](../otp_extended_testing_suite_20261115/plan.md) (Track 2), after [`runtime_codegen_hardening_20261115`](../runtime_codegen_hardening_20261115/plan.md) (Track 1) is Green.

---

## Phase 1: Per-Actor Panic Trapping, `vmem.Arena` Isolation, Fault-Contained `on_stop` & `defer_on_fault` (`spec_14`, `spec_15`, `spec_16`, `spec_20`)

- [x] Task: Implement platform `_setjmp` / `_longjmp` FFI and `Trap_Frame` trampoline (`actor/supervision.odin` & `actor/actor.odin`)
  - [x] Add OS-conditional foreign bindings (`_setjmp`/`_longjmp` on POSIX & Darwin; `_setjmp(env, nil)` on Windows x64).
  - [x] Implement `@(no_inline) run_trapped_turn` with uninitialized `frame: Trap_Frame = ---`, `_tls_trap_frame` chaining/restoration, and `actor_assertion_failure_proc` formatting into stack-inline `Fault_Info.msg_buf: [128]u8`.
  - [x] Wire `scheduler_worker_proc` and `scheduler_step_one_cooperative` to invoke turns via `run_trapped_turn` cleanly.

- [x] Task: Implement `exit_turn`, `fail_turn`, `defer_on_fault` & fault-contained `on_stop` (`actor/supervision.odin`)
  - [x] Implement `defer_on_fault(cleanup, arg)` registering onto `Trap_Frame.abort_hooks` (`MAX_ABORT_HOOKS :: 8`).
  - [x] Ensure `exit_turn()` jumps with code `2` (`.Normal`, zero abort hooks run) and `fail_turn(err_code)` jumps with code `3` (`.Error_Return`, runs abort hooks in strict LIFO order).
  - [x] Ensure `panic` / `assert(false)` / bounds-check traps run abort hooks in strict LIFO order (`Fault_Kind` preserved).
  - [x] Wrap `child_stop_proc` (`on_stop`) invocation helper inside `Trap_Frame` (`spec_20`) so panics or `fail_turn` inside `on_stop` transition the child to `.Stopped`, rewind/destroy its arena safely, and emit `Supervision_Event.Child_Stop_Fault` without unwinding out of the supervisor.

- [x] Task: Implement per-actor `vmem.Arena` isolation & fault-safe envelope flushing (`actor/actor.odin` & `actor/supervision.odin`)
  - [x] Add `Reset_Desc` to `Actor_Base` / `Actor` initialization, supporting `use_arena: bool` with small block size (`16 * mem.Kilobyte`) via `core:mem/virtual`.
  - [x] Bind `context.allocator = desc.arena_alloc` during actor turns while restoring `context.allocator = s.allocator` inside `spawn_*` calls so child actors never leak into a parent's resettable arena.
  - [x] Reclaim leaked allocations via `vmem.arena_free_all(&desc.arena)` upon actor reinit, and destroy the arena cleanly upon actor destruction.
  - [x] Ensure `actor_generic_step_proc` flushes `Batch_Return_Collector(Msg)` when aborted via fault so zero popped envelopes leak.

- [x] Task: Verify Phase 1 turns `spec_14`, `spec_15`, `spec_16` Green with zero leaks (debug & `-o:speed`)
  - [x] Run `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit -test-name:spec_14`, `spec_15`, `spec_16`.
  - [x] Verify under `-o:speed` to confirm `@(no_inline)` protects worker loop registers across `longjmp`.

- [ ] Task: Conductor - User Manual Verification 'Phase 1: Panic Trapping, Arena Isolation & defer_on_fault' (Protocol in workflow.md)

---

## Phase 2: CAF-Style Typed Monitors (`monitor`/`demonitor`), Fault-Aware `try_call` & `actor.forward` Delegation (`spec_10`–`spec_13`, `spec_16`, `spec_19`, `spec_28`)

- [x] Task: Implement thread-safe `monitor`, `demonitor` & incoming monitor edge cleanup (`actor/supervision.odin` & `actor/actor.odin`)
  - [x] Add `monitors_mu: sync.Mutex`, `monitors: [dynamic]Monitor_Link`, `next_monitor_id`, `last_exit_reason`, and `last_fault` metadata on `Actor_Base` / extension block.
  - [x] Implement `monitor(watcher: PID(Down_Event), target)` with lock-guarded `.Stopped` check and unlocked immediate `Down_Event` dispatch (late-attach CAF pattern), generating unique monotonic `Monitor_Ref.id` per call.
  - [x] Implement `demonitor(ref)` removing only the matching `Monitor_Ref.id`.
  - [x] Emit `Down_Event` notifications to all attached watchers whenever an actor terminates permanently (whether `.Normal`, `.Temporary` crash, `.Supervisor_Shutdown`, or `.Escalated`) and immediately drain/free incoming `Monitor_Node` entries (`spec_27` monitor edge hygiene).

- [x] Task: Implement fault-aware `try_call` (with dead/stale fast-fail) & `actor.forward` call delegation (`actor/actor.odin` & `actor/supervision.odin`)
  - [x] Implement `try_call` overloads returning `(Res, Call_Error)` (`sched` and ambient, `PID` and `^Actor`), checking atomic `target.status == .Stopped` (returning `({}, .Target_Stopped)`) and `pid.generation != target.generation` (returning `({}, .Stale_Generation)`) at entry before envelope allocation or parking (`spec_28`).
  - [x] On turn fault when `ctx.current_reply_slot != nil` and `(ctx.current_flags & ENVELOPE_FLAG_CALL) != 0`, wake the caller's offset-0 `Call_Slot` with error state (`3 -> Call_Target_Crashed`) before supervisor handling (`spec_12`, `spec_19`).
  - [x] Confirm `reply_ctx` clears `ctx.current_reply_slot = nil` first so replying before a subsequent panic in the same turn still yields `(res, .None)` to the caller (`spec_13`).
  - [x] Ensure `forward_*` preserves `env.reply_slot` and `ENVELOPE_FLAG_CALL` on the forwarded `Envelope` while clearing `ctx.current_reply_slot` / call flags on the router's stack `Context` so downstream worker replies or crashes directly fulfill or fail the delegated `Call_Slot` (`spec_19`).
  - [x] Drain and fail queued `ENVELOPE_FLAG_CALL` envelopes when a target actor transitions to `.Stopped` (`spec_10`, `spec_12`, `spec_28`).

- [x] Task: Verify Phase 2 turns `spec_10`, `spec_11`, `spec_12`, `spec_13`, `spec_16`, `spec_19`, and `spec_28` Green
  - [x] Run targeted `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit` checks for `spec_10`–`spec_13`, `spec_16`, `spec_19`, `spec_28`, confirming `0` leaks.

- [ ] Task: Conductor - User Manual Verification 'Phase 2: Typed Monitors, try_call & Call Delegation' (Protocol in workflow.md)

---

## Phase 3: Ordered `Supervisor` Engine, Cascading Rollback, `.Temporary` Reclamation, Shutdown Race Guard & Iterative Teardown (`spec_01`–`spec_06`, `spec_20`–`spec_24`)

- [x] Task: Implement `Supervisor`, explicit slice ordering, synchronous `supervise` / `supervise_supervisor` & policy matrix (`actor/supervision.odin`)
  - [x] Implement `Supervisor_State`, `Child_Entry`, `spawn_supervisor`, `supervise`, and `supervise_supervisor` with `children_mu: sync.Mutex` protecting child registration against concurrent crash races (`spec_01`) and preserving explicit slice insertion order (`[0 ..< len)` start, `[len - 1 ..>= 0]` stop, `spec_24`).
  - [x] Enforce the Erlang/OTP restart policy matrix (`.Permanent`, `.Transient`, `.Temporary` vs `.Normal`, `.Panic`, `.Error_Return`, `.Supervisor_Shutdown`, `.Escalated`).

- [x] Task: Implement `.One_For_One`, `.Rest_For_One`, `.One_For_All`, cascading `on_reinit` rollback & `.Temporary` bystander cleanup (`actor/supervision.odin`)
  - [x] Implement reverse-order child stopping (`on_stop` wrapped in `Trap_Frame`, `spec_20`) and forward-order reinitialization (`on_reinit` from `idx` / `0` up) for `.Rest_For_One` and `.One_For_All`, resetting arenas (`vmem.arena_free_all`), restoring initial state, and bumping `generation += 1`.
  - [x] Implement batch rollback on partial cohort `on_reinit` failure (`spec_21`): when sibling `i` fails `on_reinit`, immediately stop already-restarted siblings `0 ..< i` in reverse order via trapped `on_stop` before counting the attempt against `Restart_Window` and retrying or escalating.
  - [x] Ensure `.Temporary` bystanders stopped under `.One_For_All` / `.Rest_For_One` skip forward `on_reinit`, do not increment `restart_history` for bystander stop, and immediately release `Reset_Desc` / child arena memory (`spec_22`).
  - [x] Deduplicate stale `Child_Down` messages via `down.generation != entry.expected_gen` (`spec_05`).
  - [x] Trap panics/failures occurring inside `on_reinit` using a nested `Trap_Frame` (`spec_06`).

- [x] Task: Implement atomic shutdown race guards & iterative deep-tree teardown (`actor/supervision.odin`)
  - [x] Guard `Supervisor_Event` / `Child_Down` restart paths with atomic `SUP_RUNNING` / `SUP_STOPPING` / `SUP_STOPPED` transitions so concurrent child crashes during `stop_supervisor` or `scheduler_stop_and_destroy` decline `on_reinit` cleanly (`spec_23`).
  - [x] Implement `stop_supervisor` subtree teardown iteratively using an explicit work stack/queue (zero recursion proportional to depth) so `16+`-level chains and `256`-child flat supervisors tear down with zero stack overflow (`spec_24`).

- [x] Task: Verify Phase 3 turns `spec_01`–`spec_06`, `spec_20`–`spec_24` (supervisor tests) Green
  - [x] Run `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit` for `spec_01`–`spec_06`, `spec_20`, `spec_21`, `spec_23`, `spec_24` (and supervisor portion of `spec_22`) in debug and `-o:speed` modes.

- [ ] Task: Conductor - User Manual Verification 'Phase 3: Supervisor Strategies, Rollback & Iterative Teardown' (Protocol in workflow.md)

---

## Phase 4: Intensity Window, Quiet-Period Reset, Escalation, Child Admin APIs & Fault-Storm Property (`spec_07`–`spec_09`, `spec_25`–`spec_26`, `prop_18`)

- [x] Task: Implement unbounded sliding intensity window, quiet-period reset, meltdown & tree escalation (`actor/supervision.odin`)
  - [x] Maintain `restart_history: [dynamic]time.Tick` pruned against `max_window` using configurable `Clock_Proc` (`Virtual_Clock` / `time.tick_now()`), resetting cleanly after quiet periods (`spec_07`, `spec_08`, `spec_26`).
  - [x] On meltdown: iteratively stop all live children in reverse order with `.Supervisor_Shutdown` while keeping the `children` roster intact (`clear_roster = false`), transition supervisor to `.Stopped` with `.Escalated`, and notify parent supervisor / external monitors (`spec_09`).
  - [x] Implement `supervisor_reinit` for sub-supervisors so `parent_sup` resets `child_sup.restart_history`, transitions `SUP_RUNNING`, and restarts `child_sup`'s preserved child roster in forward order.

- [x] Task: Implement dynamic child administration APIs & `max_children` limit (`actor/supervision.odin`, `actor/doc.odin`)
  - [x] Implement `supervisor_terminate_child` (transitioning non-`.Temporary` entries to `.Terminated_By_Admin` without auto-restarting `.Permanent` specs, while removing `.Temporary` specs cleanly), `supervisor_restart_child`, `supervisor_delete_child`, `supervisor_count_children`, and `supervisor_which_children` (`spec_25`).
  - [x] Enforce `.Child_Still_Running`, `.Not_Found`, `.Supervisor_Stopping`, and `Supervisor_Config.max_children` (`.Max_Children_Reached`) guards (`spec_25`).

- [x] Task: Verify all Track 3 specs (`spec_01`–`spec_16`, `spec_19`–`spec_26`, `spec_28`, `prop_18`) and Track 1 tests pass (Green)
  - [x] Run `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors` and `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors`.
  - [x] Run `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit` in default and `-o:speed` modes, confirming `spec_01`–`spec_16`, `spec_19`–`spec_26` (supervision), `spec_28`, and `prop_18` pass with `0` failures and `0` memory leaks.

- [ ] Task: Conductor - User Manual Verification 'Phase 4: Intensity Reset, Child Admin & Fault-Storm Verification' (Protocol in workflow.md)
