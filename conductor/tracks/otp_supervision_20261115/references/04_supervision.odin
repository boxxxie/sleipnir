package actor
import "base:intrinsics"
import "base:runtime"
import "core:c/libc"
import "core:mem"
import vmem "core:mem/virtual"
import "core:sync"
import "core:time"
// =============================================================================
// 1. Per-Turn Panic Trap Frame (`setjmp` / `longjmp` + `assertion_failure_proc`)
// =============================================================================
Trap_Frame :: struct {
    prev:       ^Trap_Frame,
    jmp:        libc.jmp_buf,
    trapped:    bool,
    reason:     Exit_Reason,
    err_prefix: string,
    err_msg:    string,
    err_loc:    runtime.Source_Code_Location,
}
@(thread_local)
_tls_trap_frame: ^Trap_Frame
// Custom `context.assertion_failure_proc` active during actor `receive` execution.
// Catches `assert()`, `panic()`, slice/array bounds checks, and explicit `actor.fail()`.
actor_assertion_failure_proc :: proc(
    prefix, message: string,
    loc: runtime.Source_Code_Location,
) -> ! {
    frame := _tls_trap_frame
    if frame != nil {
        frame.trapped    = true
        frame.reason     = .Panic_Trapped
        frame.err_prefix = prefix
        frame.err_msg    = message
        frame.err_loc    = loc
        libc.longjmp(&frame.jmp, 1)
    }
    runtime.default_assertion_failure_proc(prefix, message, loc)
}
// Explicitly fail the current actor turn with a domain `Exit_Reason` (e.g. `.Invalid_State`).
fail_turn :: proc(reason: Exit_Reason = .Invalid_State, msg: string = "", loc := #caller_location) -> ! {
    frame := _tls_trap_frame
    if frame != nil {
        frame.trapped    = true
        frame.reason     = reason
        frame.err_prefix = "actor.fail_turn"
        frame.err_msg    = msg
        frame.err_loc    = loc
        libc.longjmp(&frame.jmp, 1)
    }
    runtime.default_assertion_failure_proc("actor.fail_turn", msg, loc)
}
// =============================================================================
// 2. Fault-Safe Step Procedure (Flushes `Batch_Return_Collector` on Panic)
// =============================================================================
@(private)
actor_generic_step_proc :: proc(
    $State: typeid,
    $Msg: typeid,
    $Mode: int,
) -> proc(base: ^Actor_Base, ctx: ^Context, quota: int) -> (int, bool) {
    return proc(base: ^Actor_Base, ctx: ^Context, quota: int) -> (int, bool) {
        self := cast(^Actor(State, Msg))base
        if intrinsics.atomic_load_explicit(&self.base.status, .Acquire) != ACTOR_STATUS_RUNNABLE {
            return 0, false
        }
        if quota == 0 {
            return 0, !mpsc_is_empty(&self.mailbox)
        }
        collector: Batch_Return_Collector(Msg)
        has_pools := len(self.pools) > 0
        processed := 0
        // Push a stack-local `Trap_Frame` so nested cooperative `actor.call` steps
        // on the same worker thread never overwrite the caller's `jmp_buf`.
        frame := Trap_Frame{prev = _tls_trap_frame}
        _tls_trap_frame = &frame
        saved_assert_proc := context.assertion_failure_proc
        context.assertion_failure_proc = actor_assertion_failure_proc
        defer {
            context.assertion_failure_proc = saved_assert_proc
            _tls_trap_frame = frame.prev
        }
        if libc.setjmp(&frame.jmp) == 0 {
            for processed < quota {
                env := mpsc_pop_detached(&self.mailbox)
                if env == nil {
                    break
                }
                ctx.current_env        = env
                ctx.current_reply_slot = env.reply_slot
                ctx.current_flags      = env.flags
                ctx.forwarded          = false
                // If the actor owns a `vmem.Arena`, bind it as `context.allocator` for the turn
                saved_alloc := context.allocator
                if self.base.has_arena {
                    context.allocator = vmem.arena_allocator(&self.base.arena)
                }
                when Mode == STEP_MODE_ACTOR {
                    self.receive(ctx, self, &env.msg)
                } else when Mode == STEP_MODE_STATE {
                    self.receive_state(ctx, &self.state, &env.msg)
                } else when Mode == STEP_MODE_SIMPLE {
                    self.receive_simple(&self.state, &env.msg)
                } else when Mode == STEP_MODE_VAL {
                    self.receive_val(&self.state, env.msg)
                }
                context.allocator = saved_alloc
                free_all(context.temp_allocator)
                ctx.current_env = nil
                processed += 1
                if !ctx.forwarded {
                    if (env.flags & ENVELOPE_FLAG_HEAP) != 0 {
                        pid := env.pool_id
                        alloc := self.pools[pid].allocator if (has_pools && int(pid) < len(self
                        free(env, alloc)
                    } else if has_pools && (env.flags & ENVELOPE_FLAG_STATIC) == 0 {
                        collector_add(&collector, env)
                    }
                }
            }
        } else {
            // --- PANIC / FAULT RECOVERY PATH ---
            // 1. Suspend actor so the Dekker re-check does not reschedule it before its supervisor act
            intrinsics.atomic_store_explicit(&self.base.status, ACTOR_STATUS_FAULTED, .Seq_Cst)
            // 2. Wipe per-turn scratch memory
            free_all(context.temp_allocator)
            // 3. If the faulting message was a synchronous `actor.call`, wake the caller with failure
            if (ctx.current_flags & ENVELOPE_FLAG_CALL) != 0 && ctx.current_reply_slot != nil {
                ready_futex := cast(^sync.Futex)ctx.current_reply_slot
                intrinsics.atomic_store_explicit((^u32)(ready_futex), CALL_READY_FAILED, .Release)
                sync.futex_signal(ready_futex)
            }
            // 4. Recycle the faulting envelope so `Envelope_Pool(Msg)` never leaks a slot!
            if ctx.current_env != nil && !ctx.forwarded {
                env := cast(^Envelope(Msg))ctx.current_env
                if (env.flags & ENVELOPE_FLAG_HEAP) != 0 {
                    pid := env.pool_id
                    alloc := self.pools[pid].allocator if (has_pools && int(pid) < len(self.pools)
                    free(env, alloc)
                } else if has_pools && (env.flags & ENVELOPE_FLAG_STATIC) == 0 {
                    collector_add(&collector, env)
                }
                ctx.current_env = nil
            }
            // 5. Flush all envelopes collected prior to (and including) the crash
            if has_pools {
                collector_flush(&collector, self.pools, ctx.worker_id)
            }
            // 6. Notify the attached supervisor actor (if any)
            if self.base.supervisor != nil {
                sup_ref := cast(^Actor_Ref(Supervisor_Msg))self.base.supervisor
                send_ctx_pid(ctx, PID(Supervisor_Msg){ref = sup_ref}, Supervisor_Msg(Child_Down{
                    child = &self.base,
                    fault = Fault_Info{
                        actor_id   = self.base.id,
                        generation = intrinsics.atomic_load_explicit(&self.base.generation, .Re
                        reason     = frame.reason,
                        prefix     = frame.err_prefix,
                        message    = frame.err_msg,
                        loc        = frame.err_loc,
                    },
                }))
            }
            return processed, false
        }
        if processed > 0 && has_pools {
            collector_flush(&collector, self.pools, ctx.worker_id)
        }
        return processed, !mpsc_is_empty(&self.mailbox)
    }
}
// =============================================================================
// 3. OTP Supervisor Actor (`.One_For_One`, `.One_For_All`, `.Rest_For_One`)
// =============================================================================
Restart_Strategy :: enum u8 {
    One_For_One,  // Restart only the crashed child
    One_For_All,  // Restart all children managed by this supervisor
    Rest_For_One, // Restart the crashed child and any children registered after it
}
Restart_Policy :: enum u8 {
    Permanent, // Always restart on any exit
    Transient, // Restart only on abnormal Exit_Reason
    Temporary, // Never restart (mark STOPPED)
}
Child_Down :: struct {
    child: ^Actor_Base,
    fault: Fault_Info,
}
Supervise_Child :: struct {
    child:  ^Actor_Base,
    policy: Restart_Policy,
}
Get_Supervisor_Stats :: struct {}
Supervisor_Stats :: struct {
    total_restarts: int,
    escalated:      bool,
    last_fault:     Fault_Info,
}
Supervisor_Msg :: union {
    Supervise_Child,
    Child_Down,
    Get_Supervisor_Stats,
}
Child_Entry :: struct {
    base:     ^Actor_Base,
    policy:   Restart_Policy,
    restarts: int,
}
Supervisor_State :: struct {
    strategy:        Restart_Strategy,
    max_restarts:    int,
    within_window:   time.Duration,
    restart_history: [dynamic; 16]time.Tick, // Inline fixed-capacity ring; zero heap alloc!
    children:        [dynamic; 32]Child_Entry,
    total_restarts:  int,
    escalated:       bool,
    last_fault:      Fault_Info,
    on_escalate:     proc(sup: ^Supervisor_State, fault: Fault_Info),
}
// Enable a `vmem.Arena` and automatic state reset on an existing `^Actor($State, $Msg)`.
// When the actor faults, its `vmem.Arena` is wiped in O(1), `state` is reset to `initial_state`
// (plus optional `on_reinit`), and any remaining mailbox messages are preserved or drained.
enable_actor_supervision :: proc(
    a: ^Actor($State, $Msg),
    initial_state: State,
    on_reinit: proc(state: ^State, arena_alloc: mem.Allocator) = nil,
    use_arena: bool = true,
    keep_mailbox: bool = true,
) -> mem.Allocator_Error {
    if use_arena && !a.base.has_arena {
        vmem.arena_init_growing(&a.base.arena) or_return
        a.base.has_arena = true
    }
    a.base.keep_mailbox = keep_mailbox
    // Store `initial_state` + `on_reinit` in a small descriptor on `a.allocator`
    Reset_Desc :: struct {
        init_state: State,
        reinit_fn:  proc(state: ^State, arena_alloc: mem.Allocator),
    }
    desc := new(Reset_Desc, a.allocator) or_return
    desc.init_state = initial_state
    desc.reinit_fn  = on_reinit
    a.base.user_ptr = desc
    a.base.reset_proc = proc(base: ^Actor_Base) {
        self := cast(^Actor(State, Msg))base
        d := cast(^Reset_Desc)base.user_ptr
        // 1. Drain mailbox back to `self.pools` if `keep_mailbox == false`
        if !base.keep_mailbox {
            collector: Batch_Return_Collector(Msg)
            has_pools := len(self.pools) > 0
            for {
                env := mpsc_pop_detached(&self.mailbox)
                if env == nil {
                    break
                }
                if (env.flags & ENVELOPE_FLAG_HEAP) != 0 {
                    free(env, self.allocator)
                } else if has_pools && (env.flags & ENVELOPE_FLAG_STATIC) == 0 {
                    collector_add(&collector, env)
                }
            }
            if has_pools {
                collector_flush(&collector, self.pools, -1)
            }
        }
        // 2. O(1) wipe of the actor's private virtual memory arena
        if base.has_arena {
            vmem.arena_free_all(&base.arena)
        }
        // 3. Restore clean initial state and run optional `xitdb` rehydration callback
        if d != nil {
            self.state = d.init_state
            if d.reinit_fn != nil {
                alloc := vmem.arena_allocator(&base.arena) if base.has_arena else self.allocator
                d.reinit_fn(&self.state, alloc)
            }
        }
    }
    // Ensure `destroy_proc` frees `Reset_Desc` and `vmem.Arena` on scheduler shutdown
    a.base.destroy_proc = proc(base: ^Actor_Base) {
        self := cast(^Actor(State, Msg))base
        if base.user_ptr != nil {
            free(base.user_ptr, self.allocator)
            base.user_ptr = nil
        }
        if base.has_arena {
            vmem.arena_destroy(&base.arena)
            base.has_arena = false
        }
        free(self, self.allocator)
    }
    if a.base.has_arena && on_reinit != nil {
        on_reinit(&a.state, vmem.arena_allocator(&a.base.arena))
    }
    return nil
}
@(private)
should_escalate_crash :: proc(sup: ^Supervisor_State) -> bool {
    now := time.tick_now()
    cutoff := time.tick_add(now, -sup.within_window)
    write_idx := 0
    for i := 0; i < len(sup.restart_history); i += 1 {
        tick := sup.restart_history[i]
        if time.tick_diff(cutoff, tick) <= 0 {
            sup.restart_history[write_idx] = tick
            write_idx += 1
        }
    }
    resize(&sup.restart_history, write_idx)
    if len(sup.restart_history) >= sup.max_restarts {
        return true
    }
    append(&sup.restart_history, now)
    return false
}
@(private)
restart_child_entry :: proc(ctx: ^Context, entry: ^Child_Entry) {
    child := entry.base
    if child == nil {
        return
    }
    // Wait until the worker thread that faulted finishes unscheduling `child.scheduled`
    for intrinsics.atomic_load_explicit(&child.scheduled, .Acquire) != 0 {
        intrinsics.cpu_relax()
    }
    intrinsics.atomic_store_explicit(&child.status, ACTOR_STATUS_FAULTED, .Seq_Cst)
    if child.reset_proc != nil {
        child.reset_proc(child)
    }
    intrinsics.atomic_add_explicit(&child.generation, 1, .Release)
    intrinsics.atomic_store_explicit(&child.status, ACTOR_STATUS_RUNNABLE, .Seq_Cst)
    entry.restarts += 1
    // If messages are waiting in `child.mailbox`, schedule `child` immediately
    _, has_pending := child.step_proc(child, ctx, 0)
    if has_pending {
        scheduler_schedule_actor(ctx.sched, child, ctx.worker_id)
    }
}
supervisor_receive :: proc(
    ctx: ^Context,
    self: ^Actor(Supervisor_State, Supervisor_Msg),
    msg: ^Supervisor_Msg,
) {
    sup := &self.state
    switch m in msg^ {
    case Supervise_Child:
        m.child.supervisor = &self.ref
        append(&sup.children, Child_Entry{
            base   = m.child,
            policy = m.policy,
        })
    case Get_Supervisor_Stats:
        reply(ctx, Supervisor_Stats{
            total_restarts = sup.total_restarts,
            escalated      = sup.escalated,
            last_fault     = sup.last_fault,
        })
    case Child_Down:
        sup.last_fault = m.fault
        idx := -1
        for i := 0; i < len(sup.children); i += 1 {
            if sup.children[i].base == m.child {
                idx = i
                break
            }
        }
        if idx < 0 {
            return
        }
        policy := sup.children[idx].policy
        if policy == .Temporary || (policy == .Transient && m.fault.reason == .Normal) {
            intrinsics.atomic_store_explicit(&m.child.status, ACTOR_STATUS_STOPPED, .Seq_Cst)
            return
        }
        if should_escalate_crash(sup) {
            sup.escalated = true
            for i := 0; i < len(sup.children); i += 1 {
                intrinsics.atomic_store_explicit(&sup.children[i].base.status, ACTOR_STATUS_STOPPED,
            }
            if sup.on_escalate != nil {
                sup.on_escalate(sup, m.fault)
            }
            // If this supervisor itself has a parent supervisor, escalate by failing turn!
            if self.base.supervisor != nil {
                fail_turn(m.fault.reason, m.fault.message, m.fault.loc)
            }
            return
        }
        switch sup.strategy {
        case .One_For_One:
            restart_child_entry(ctx, &sup.children[idx])
            sup.total_restarts += 1
        case .Rest_For_One:
            for i := idx; i < len(sup.children); i += 1 {
                restart_child_entry(ctx, &sup.children[i])
                sup.total_restarts += 1
            }
        case .One_For_All:
            for i := 0; i < len(sup.children); i += 1 {
                restart_child_entry(ctx, &sup.children[i])
                sup.total_restarts += 1
            }
        }
    }
}
// Spawn an OTP-style Supervisor actor on the current or specified `Scheduler`.
@(require_results)
spawn_supervisor :: proc(
    strategy: Restart_Strategy = .One_For_One,
    max_restarts: int = 5,
    within_window: time.Duration = 10 * time.Second,
    on_escalate: proc(sup: ^Supervisor_State, fault: Fault_Info) = nil,
    s: ^Scheduler = nil,
    allocator := context.allocator,
) -> ^Actor(Supervisor_State, Supervisor_Msg) {
    init_state := Supervisor_State{
        strategy      = strategy,
        max_restarts  = max_restarts,
        within_window = within_window,
        on_escalate   = on_escalate,
    }
    sched := s if s != nil else current_scheduler()
    a, _ := spawn(sched, init_state, supervisor_receive, 0, allocator)
    return a
}
// Attach a child actor to `sup` with its initial state and optional `on_reinit` hook.
supervise :: proc(
    sup: ^Actor(Supervisor_State, Supervisor_Msg),
    child: ^Actor($State, $Msg),
    initial_state: State,
    policy: Restart_Policy = .Permanent,
    on_reinit: proc(state: ^State, arena_alloc: mem.Allocator) = nil,
    use_arena: bool = true,
    keep_mailbox: bool = true,
) {
    _ = enable_actor_supervision(child, initial_state, on_reinit, use_arena, keep_mailbox)
    child.base.supervisor = &sup.ref
    send(sup, Supervisor_Msg(Supervise_Child{
        child  = &child.base,
        policy = policy,
    }))
}
