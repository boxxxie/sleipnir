package actor

import "base:intrinsics"
import "base:runtime"
import "core:c"
import "core:mem"
import vmem "core:mem/virtual"
import "core:strings"
import "core:sync"
import "core:time"

// =============================================================================
// Layer 4 OTP Supervision, Fault Control, Monitors, Registry & Multi-Tenant API
// =============================================================================

// Public allocator alias so `@(reinit)` handlers and generated code can reference `actor.Allocator`
// without requiring callers to import `core:mem`.
Allocator :: mem.Allocator

Actor_Status :: enum u32 {
	Running             = 0,
	Stopping            = 1,
	Faulted             = 2,
	Stopped             = 3,
	Terminated_By_Admin = 4,
}

Fault_Kind :: enum u8 {
	None             = 0,
	Panic            = 1,
	Assert           = 2,
	Bounds_Check     = 3,
	Error_Return     = 4,
	Child_Stop_Fault = 5,
}

Exit_Reason :: enum u8 {
	Normal              = 0,
	Panic               = 1,
	Error_Return        = 2,
	Supervisor_Shutdown = 3,
	Escalated           = 4,
}

Call_Error :: enum u8 {
	None                = 0,
	Call_Target_Crashed = 1,
	Deadlock_Detected   = 2,
	Timeout             = 3,
	Target_Stopped      = 4,
	Stale_Generation    = 5,
}

Fault_Info :: struct {
	actor_id:   u32,
	generation: u32,
	kind:       Fault_Kind,
	reason:     Exit_Reason,
	error_code: i32,
	msg_len:    u8,
	msg_buf:    [128]u8,
	loc:        runtime.Source_Code_Location,
}

fault_message :: proc(f: ^Fault_Info) -> string {
	if f == nil || f.msg_len == 0 {
		return ""
	}
	n := min(int(f.msg_len), len(f.msg_buf))
	return string(f.msg_buf[:n])
}

// -----------------------------------------------------------------------------
// Platform `_setjmp` / `_longjmp` FFI & Per-Worker `Trap_Frame`
// -----------------------------------------------------------------------------

Jmp_Buf :: struct #align(16) {
	_: [64]u64,
}

when ODIN_OS == .Windows {
	foreign import _sup_libc "system:libucrt.lib"
	@(default_calling_convention = "c")
	foreign _sup_libc {
		@(link_name = "_setjmp")
		trap_setjmp_win :: proc(env: ^Jmp_Buf, frame: rawptr = nil) -> c.int ---
		@(link_name = "longjmp")
		trap_longjmp_win :: proc(env: ^Jmp_Buf, val: c.int) -> ! ---
	}
} else when ODIN_OS == .Darwin {
	foreign import _sup_libc "system:System"
	@(default_calling_convention = "c")
	foreign _sup_libc {
		@(link_name = "_setjmp")
		trap_setjmp_posix :: proc(env: ^Jmp_Buf) -> c.int ---
		@(link_name = "_longjmp")
		trap_longjmp_posix :: proc(env: ^Jmp_Buf, val: c.int) -> ! ---
	}
} else {
	foreign import _sup_libc "system:c"
	@(default_calling_convention = "c")
	foreign _sup_libc {
		@(link_name = "_setjmp")
		trap_setjmp_posix :: proc(env: ^Jmp_Buf) -> c.int ---
		@(link_name = "_longjmp")
		trap_longjmp_posix :: proc(env: ^Jmp_Buf, val: c.int) -> ! ---
	}
}

@(private)
trap_longjmp :: #force_inline proc(env: ^Jmp_Buf, val: c.int) -> ! {
	when ODIN_OS == .Windows {
		trap_longjmp_win(env, val)
	} else {
		trap_longjmp_posix(env, val)
	}
}

MAX_ABORT_HOOKS :: 8

@(private)
Abort_Hook_Entry :: struct {
	arg:     rawptr,
	cleanup: proc(rawptr),
}

@(private)
Trap_Frame :: struct {
	jmp:          Jmp_Buf,
	active:       bool,
	fault:        Fault_Info,
	hook_count:   int,
	hooks:        [MAX_ABORT_HOOKS]Abort_Hook_Entry,
	prev:         ^Trap_Frame,
	arena_data:   rawptr,
	saved_alloc:  mem.Allocator,
	saved_assert: runtime.Assertion_Failure_Proc,
}

@(thread_local)
_tls_trap_frame: ^Trap_Frame

// Returns true when an active `Trap_Frame` is installed on the current worker thread.
is_trap_active :: #force_inline proc() -> bool {
	return _tls_trap_frame != nil && _tls_trap_frame.active
}

@(private)
resolve_spawn_allocator :: #force_inline proc(s: ^Scheduler, allocator: mem.Allocator) -> mem.Allocator {
	if s != nil && _tls_trap_frame != nil && _tls_trap_frame.arena_data != nil && allocator.data == _tls_trap_frame.arena_data {
		return s.allocator
	}
	return allocator
}

@(private)
copy_fault_msg :: proc(info: ^Fault_Info, s: string) {
	n := min(len(s), len(info.msg_buf))
	if n > 0 {
		mem.copy(&info.msg_buf[0], raw_data(s), n)
	}
	info.msg_len = u8(n)
}

@(private)
run_abort_hooks :: proc(frame: ^Trap_Frame) {
	for i := frame.hook_count - 1; i >= 0; i -= 1 {
		hook := frame.hooks[i]
		if hook.cleanup != nil {
			hook.cleanup(hook.arg)
		}
	}
	frame.hook_count = 0
}

@(private)
actor_assertion_failure_proc :: proc(prefix, message: string, loc: runtime.Source_Code_Location) -> ! {
	if f := _tls_trap_frame; f != nil && f.active {
		kind: Fault_Kind = .Panic
		if prefix == "runtime assertion" {
			kind = .Assert
		} else if strings.contains(message, "bounds") || strings.contains(prefix, "bounds") {
			kind = .Bounds_Check
		}
		actor_id := u32(0)
		gen := u32(0)
		if ctx := _tls_active_ctx; ctx != nil && ctx.current_actor != nil {
			actor_id = ctx.current_actor.id
			gen = intrinsics.atomic_load_explicit(&ctx.current_actor.generation, .Acquire)
		}
		f.fault = Fault_Info{
			actor_id   = actor_id,
			generation = gen,
			kind       = kind,
			reason     = .Panic,
			loc        = loc,
		}
		copy_fault_msg(&f.fault, message if len(message) > 0 else prefix)
		trap_longjmp(&f.jmp, 1)
	}
	runtime.default_assertion_failure_proc(prefix, message, loc)
}

@(private)
dispatch_actor_turn :: #force_no_inline proc(
	self: ^Actor($State, $Msg),
	env:  ^Envelope(Msg),
	ctx:  ^Context,
	$Mode: int,
) {
	when Mode == STEP_MODE_ACTOR {
		self.receive(ctx, self, &env.msg)
	} else when Mode == STEP_MODE_STATE {
		self.receive_state(ctx, &self.state, &env.msg)
	} else when Mode == STEP_MODE_SIMPLE {
		self.receive_simple(&self.state, &env.msg)
	} else when Mode == STEP_MODE_VAL {
		self.receive_val(&self.state, env.msg)
	}
}

@(private)
execute_trapped_actor_turn :: #force_no_inline proc(
	self: ^Actor($State, $Msg),
	env:  ^Envelope(Msg),
	ctx:  ^Context,
	$Mode: int,
) -> (trapped: bool, normal_exit: bool, fault: Fault_Info) {
	frame: Trap_Frame = ---
	frame.active       = true
	frame.fault        = {}
	frame.hook_count   = 0
	frame.prev         = _tls_trap_frame
	frame.saved_alloc  = context.allocator
	frame.saved_assert = context.assertion_failure_proc
	frame.arena_data   = nil
	turn_alloc         := context.allocator
	if ext := get_actor_ext(&self.base); ext != nil && ext.use_arena && ext.arena_initialized {
		turn_alloc       = ext.arena_alloc
		frame.arena_data = ext.arena_alloc.data
	}
	context.allocator              = turn_alloc
	context.assertion_failure_proc = actor_assertion_failure_proc
	_tls_trap_frame = &frame

	when ODIN_OS == .Windows {
		code := trap_setjmp_win(&frame.jmp, nil)
	} else {
		code := trap_setjmp_posix(&frame.jmp)
	}

	if code == 0 {
		dispatch_actor_turn(self, env, ctx, Mode)
		_tls_trap_frame                = frame.prev
		context.assertion_failure_proc = frame.saved_assert
		context.allocator              = frame.saved_alloc
		return false, false, {}
	}

	_tls_trap_frame                = frame.prev
	context.assertion_failure_proc = frame.saved_assert
	context.allocator              = frame.saved_alloc
	if code != 2 {
		run_abort_hooks(&frame)
	}
	return true, (code == 2), frame.fault
}

@(require_results)
actor_status :: proc(a: ^Actor($State, $Msg)) -> Actor_Status {
	if a == nil {
		return .Stopped
	}
	return Actor_Status(intrinsics.atomic_load_explicit(&a.base.status, .Acquire))
}

@(require_results)
actor_generation :: proc(a: ^Actor($State, $Msg)) -> u32 {
	if a == nil {
		return 0
	}
	return intrinsics.atomic_load_explicit(&a.base.generation, .Acquire)
}

exit_turn :: proc(reason: Exit_Reason = .Normal, msg: string = "", loc := #caller_location) {
	f := _tls_trap_frame
	if f == nil || !f.active {
		return
	}
	actor_id := u32(0)
	gen := u32(0)
	if ctx := _tls_active_ctx; ctx != nil && ctx.current_actor != nil {
		actor_id = ctx.current_actor.id
		gen = intrinsics.atomic_load_explicit(&ctx.current_actor.generation, .Acquire)
	}
	if reason == .Normal {
		f.fault = Fault_Info{
			actor_id   = actor_id,
			generation = gen,
			kind       = .None,
			reason     = .Normal,
			loc        = loc,
		}
		copy_fault_msg(&f.fault, msg)
		trap_longjmp(&f.jmp, 2)
	}
	kind: Fault_Kind = .Error_Return if reason == .Error_Return else .Panic
	f.fault = Fault_Info{
		actor_id   = actor_id,
		generation = gen,
		kind       = kind,
		reason     = reason,
		loc        = loc,
	}
	copy_fault_msg(&f.fault, msg)
	trap_longjmp(&f.jmp, 3)
}

fail_turn_code :: proc(err_code: i32 = 1, msg: string = "", loc := #caller_location) {
	f := _tls_trap_frame
	if f == nil || !f.active {
		return
	}
	actor_id := u32(0)
	gen := u32(0)
	if ctx := _tls_active_ctx; ctx != nil && ctx.current_actor != nil {
		actor_id = ctx.current_actor.id
		gen = intrinsics.atomic_load_explicit(&ctx.current_actor.generation, .Acquire)
	}
	f.fault = Fault_Info{
		actor_id   = actor_id,
		generation = gen,
		kind       = .Error_Return,
		reason     = .Error_Return,
		error_code = err_code,
		loc        = loc,
	}
	copy_fault_msg(&f.fault, msg)
	trap_longjmp(&f.jmp, 3)
}

fail_turn_reason :: proc(reason: Exit_Reason, msg: string = "", loc := #caller_location) {
	f := _tls_trap_frame
	if f == nil || !f.active {
		return
	}
	actor_id := u32(0)
	gen := u32(0)
	if ctx := _tls_active_ctx; ctx != nil && ctx.current_actor != nil {
		actor_id = ctx.current_actor.id
		gen = intrinsics.atomic_load_explicit(&ctx.current_actor.generation, .Acquire)
	}
	kind: Fault_Kind = .Error_Return if reason == .Error_Return else .Panic
	f.fault = Fault_Info{
		actor_id   = actor_id,
		generation = gen,
		kind       = kind,
		reason     = reason,
		error_code = 1,
		loc        = loc,
	}
	copy_fault_msg(&f.fault, msg)
	trap_longjmp(&f.jmp, 3)
}

fail_turn :: proc {
	fail_turn_code,
	fail_turn_reason,
}

defer_on_fault_proc_first :: proc(cleanup: proc(rawptr), arg: rawptr) {
	f := _tls_trap_frame
	if f == nil || !f.active || cleanup == nil {
		return
	}
	if f.hook_count < MAX_ABORT_HOOKS {
		f.hooks[f.hook_count] = Abort_Hook_Entry{arg = arg, cleanup = cleanup}
		f.hook_count += 1
	}
}

defer_on_fault_arg_first :: proc(arg: rawptr, cleanup: proc(rawptr)) {
	defer_on_fault_proc_first(cleanup, arg)
}

defer_on_fault :: proc {
	defer_on_fault_proc_first,
	defer_on_fault_arg_first,
}

// -----------------------------------------------------------------------------
// Per-Actor Supervision Extension (`Reset_Desc` + Monitors + Registry Hooks)
// -----------------------------------------------------------------------------

@(private)
Monitor_Link :: struct {
	id:           u64,
	watcher_base: ^Actor_Base,
	watcher_gen:  u32,
	deliver_proc: proc(watcher_base: ^Actor_Base, watcher_gen: u32, ev: Down_Event),
}

@(private)
Registry_Cleanup_Hook :: struct {
	reg_ptr:      rawptr,
	cleanup_proc: proc(reg_ptr: rawptr, base: ^Actor_Base),
}

@(private)
Actor_Extension :: struct {
	allocator:         mem.Allocator,
	is_supervised:     bool,
	sup:               ^Supervisor,
	child_id:          u64,
	state_offset:      uintptr,
	state_size:        int,
	init_state_copy:   rawptr,
	use_arena:         bool,
	keep_mailbox:      bool,
	arena_initialized: bool,
	arena:             vmem.Arena,
	arena_alloc:       mem.Allocator,
	user_on_reinit:    rawptr,
	user_on_stop:      rawptr,
	reinit_proc:       proc(base: ^Actor_Base, fn_ptr: rawptr, arena_alloc: mem.Allocator),
	stop_proc:         proc(base: ^Actor_Base, fn_ptr: rawptr, reason: Exit_Reason),
	drain_proc:        proc(base: ^Actor_Base),
	has_messages_proc: proc(base: ^Actor_Base) -> bool,
	calling_target:    ^Actor_Base,
	monitors_mu:       sync.Mutex,
	monitors:          [dynamic]Monitor_Link,
	next_monitor_id:   u64,
	finalized:         u32,
	stopped_drained:   u32,
	last_exit_reason:  Exit_Reason,
	last_fault:        Fault_Info,
	reg_hooks:         [dynamic]Registry_Cleanup_Hook,
	sup_internal:      ^Supervisor_Internal,
}

@(private)
get_actor_ext :: #force_inline proc(base: ^Actor_Base) -> ^Actor_Extension {
	if base == nil {
		return nil
	}
	return cast(^Actor_Extension)intrinsics.atomic_load_explicit((^rawptr)(&base.user_ptr), .Acquire)
}

@(private)
resolve_ext_allocator :: #force_inline proc(base: ^Actor_Base, fallback: mem.Allocator) -> mem.Allocator {
	if base != nil && base.reg_sched != nil && base.reg_sched.allocator.procedure != nil {
		return base.reg_sched.allocator
	}
	if fallback.procedure != nil {
		return fallback
	}
	return context.allocator
}

@(private)
ensure_actor_ext :: proc(base: ^Actor_Base, allocator := context.allocator) -> ^Actor_Extension {
	if base == nil {
		return nil
	}
	if existing := get_actor_ext(base); existing != nil {
		return existing
	}
	alloc := resolve_ext_allocator(base, allocator)
	ext := new(Actor_Extension, alloc)
	ext.allocator = alloc
	ext.monitors  = make([dynamic]Monitor_Link, 0, 0, alloc)
	ext.reg_hooks = make([dynamic]Registry_Cleanup_Hook, 0, 0, alloc)
	prev, ok := intrinsics.atomic_compare_exchange_strong_explicit(
		(^rawptr)(&base.user_ptr),
		nil,
		rawptr(ext),
		.Acq_Rel,
		.Acquire,
	)
	if !ok {
		delete(ext.monitors)
		delete(ext.reg_hooks)
		free(ext, alloc)
		return cast(^Actor_Extension)prev
	}
	return ext
}

@(private)
free_actor_reset_resources :: proc(ext: ^Actor_Extension) {
	if ext == nil {
		return
	}
	if ext.init_state_copy != nil {
		mem.free(ext.init_state_copy, ext.allocator)
		ext.init_state_copy = nil
	}
	if ext.arena_initialized {
		vmem.arena_destroy(&ext.arena)
		ext.arena_initialized = false
		ext.arena_alloc = {}
	}
}

@(private)
actor_cleanup_supervision :: proc(base: ^Actor_Base) {
	if base == nil {
		return
	}
	raw := intrinsics.atomic_exchange_explicit((^rawptr)(&base.user_ptr), nil, .Acq_Rel)
	if raw == nil {
		return
	}
	ext := cast(^Actor_Extension)raw

	sync.mutex_lock(&ext.monitors_mu)
	local_hooks := ext.reg_hooks
	ext.reg_hooks = {}
	sync.mutex_unlock(&ext.monitors_mu)
	for hook in local_hooks {
		if hook.cleanup_proc != nil {
			hook.cleanup_proc(hook.reg_ptr, base)
		}
	}
	delete(local_hooks)

	if ext.sup_internal != nil {
		si := ext.sup_internal
		ext.sup_internal = nil
		delete(si.children)
		delete(si.restart_history)
		free(si, ext.allocator)
	}

	free_actor_reset_resources(ext)
	delete(ext.monitors)
	delete(ext.reg_hooks)
	free(ext, ext.allocator)
}

@(private)
actor_drain_stopped_target :: proc(base: ^Actor_Base, wait_if_busy: bool = true) {
	if base == nil {
		return
	}
	ext := get_actor_ext(base)
	if ext == nil {
		return
	}
	is_self_step := _tls_active_ctx != nil && _tls_active_ctx.current_actor == base
	if wait_if_busy && !is_self_step {
		for intrinsics.atomic_load_explicit(&base.in_step, .Seq_Cst) != 0 {
			intrinsics.cpu_relax()
		}
		for intrinsics.atomic_load_explicit(&ext.finalized, .Acquire) != 0 &&
		    intrinsics.atomic_load_explicit(&ext.stopped_drained, .Acquire) == 0 {
			intrinsics.cpu_relax()
		}
	}
	if intrinsics.atomic_load_explicit(&ext.stopped_drained, .Seq_Cst) == 0 {
		// `finalize_actor_stopped` has not drained this target yet and will drain all
		// already-pushed envelopes once the target exits its active turn.
		return
	}
	sync.mutex_lock(&ext.monitors_mu)
	if ext.drain_proc != nil {
		ext.drain_proc(base)
	}
	sync.mutex_unlock(&ext.monitors_mu)
}

@(private)
finalize_actor_stopped :: proc(base: ^Actor_Base, reason: Exit_Reason, fault: Fault_Info) {
	if base == nil {
		return
	}
	ext := ensure_actor_ext(base)

	sync.mutex_lock(&ext.monitors_mu)
	if intrinsics.atomic_load_explicit(&ext.finalized, .Acquire) != 0 {
		if ext.drain_proc != nil {
			ext.drain_proc(base)
		}
		sync.mutex_unlock(&ext.monitors_mu)
		return
	}
	intrinsics.atomic_store_explicit(&ext.finalized, 1, .Release)
	ext.last_exit_reason = reason
	ext.last_fault       = fault
	intrinsics.atomic_store_explicit(&base.status, u32(Actor_Status.Stopped), .Seq_Cst)
	sync.mutex_unlock(&ext.monitors_mu)

	is_self_step := _tls_active_ctx != nil && _tls_active_ctx.current_actor == base
	if !is_self_step {
		for intrinsics.atomic_load_explicit(&base.in_step, .Seq_Cst) != 0 {
			intrinsics.cpu_relax()
		}
	}

	sync.mutex_lock(&ext.monitors_mu)
	// Publish `stopped_drained` BEFORE calling `drain_proc` under `monitors_mu` so concurrent
	// async senders either observe `0` before `drain_proc` starts or `1` and drain under `monitors_mu`.
	intrinsics.atomic_store_explicit(&ext.stopped_drained, 1, .Seq_Cst)
	if ext.drain_proc != nil {
		ext.drain_proc(base)
	}
	local_mons  := ext.monitors
	local_hooks := ext.reg_hooks
	ext.monitors  = {}
	ext.reg_hooks = {}
	sync.mutex_unlock(&ext.monitors_mu)

	for hook in local_hooks {
		if hook.cleanup_proc != nil {
			hook.cleanup_proc(hook.reg_ptr, base)
		}
	}
	delete(local_hooks)

	gen := intrinsics.atomic_load_explicit(&base.generation, .Acquire)
	for m in local_mons {
		if m.deliver_proc != nil && m.watcher_base != nil {
			ev := Down_Event{
				ref        = Monitor_Ref{id = m.id, target_base = base},
				actor_id   = base.id,
				pid_raw    = base,
				generation = gen,
				reason     = reason,
				fault      = fault,
			}
			m.deliver_proc(m.watcher_base, m.watcher_gen, ev)
		}
	}
	delete(local_mons)
}

@(private)
handle_actor_turn_exit :: proc(base: ^Actor_Base, normal_exit: bool, fault: Fault_Info) {
	reason: Exit_Reason
	if normal_exit {
		reason = .Normal
	} else if fault.kind == .Error_Return {
		reason = .Error_Return
	} else {
		reason = .Panic
	}

	_, ok := intrinsics.atomic_compare_exchange_strong_explicit(
		&base.status,
		u32(Actor_Status.Running),
		u32(Actor_Status.Faulted),
		.Acq_Rel,
		.Acquire,
	)
	if !ok {
		return
	}

	ext := get_actor_ext(base)
	if ext != nil && ext.is_supervised && ext.sup != nil {
		sup := ext.sup
		si := get_sup_internal(sup)
		if si != nil && intrinsics.atomic_load_explicit(&si.sup_state, .Acquire) == SUP_RUNNING {
			if intrinsics.atomic_load_explicit(&sup.base.status, .Acquire) == u32(Actor_Status.Running) {
				s := resolve_target_scheduler(nil, &sup.ref)
				if s != nil {
					gen := intrinsics.atomic_load_explicit(&base.generation, .Acquire)
					send_sched_auto(s, sup, Supervisor_Msg(Child_Down{
						child_id   = ext.child_id,
						generation = gen,
						reason     = reason,
						fault      = fault,
					}))
					return
				}
			}
		}
	}

	finalize_actor_stopped(base, reason, fault)
}

// -----------------------------------------------------------------------------
// CAF-Style Typed Monitors & Fault-Aware `try_call`
// -----------------------------------------------------------------------------

Monitor_Ref :: struct {
	id:          u64,
	target_base: ^Actor_Base,
}

Down_Event :: struct {
	ref:        Monitor_Ref,
	actor_id:   u32,
	pid_raw:    rawptr,
	generation: u32,
	reason:     Exit_Reason,
	fault:      Fault_Info,
}

@(private)
deliver_down_to_watcher :: proc(watcher_base: ^Actor_Base, watcher_gen: u32, ev: Down_Event) {
	if watcher_base == nil || !actor_base_can_accept(watcher_base, watcher_gen) {
		return
	}
	watcher_ref := cast(^Actor_Ref(Down_Event))watcher_base
	s := resolve_target_scheduler(nil, watcher_ref)
	if s == nil {
		return
	}
	send_sched_auto_pid(s, PID(Down_Event){ref = watcher_ref, generation = watcher_gen}, ev)
}

@(private)
monitor_impl :: proc(watcher: PID(Down_Event), target_base: ^Actor_Base, target_gen: u32) -> Monitor_Ref {
	if watcher.ref == nil || target_base == nil {
		return {}
	}
	ext := ensure_actor_ext(target_base)
	sync.mutex_lock(&ext.monitors_mu)
	ext.next_monitor_id += 1
	id := ext.next_monitor_id
	cur_status := intrinsics.atomic_load_explicit(&target_base.status, .Acquire)
	cur_gen    := intrinsics.atomic_load_explicit(&target_base.generation, .Acquire)
	if cur_status == u32(Actor_Status.Stopped) || (target_gen != 0 && target_gen != cur_gen) {
		reason := ext.last_exit_reason
		fault  := ext.last_fault
		sync.mutex_unlock(&ext.monitors_mu)
		deliver_down_to_watcher(&watcher.ref.base, watcher.generation, Down_Event{
			ref        = Monitor_Ref{id = id, target_base = target_base},
			actor_id   = target_base.id,
			pid_raw    = target_base,
			generation = cur_gen,
			reason     = reason,
			fault      = fault,
		})
		return Monitor_Ref{id = id, target_base = target_base}
	}
	append(&ext.monitors, Monitor_Link{
		id           = id,
		watcher_base = &watcher.ref.base,
		watcher_gen  = watcher.generation,
		deliver_proc = deliver_down_to_watcher,
	})
	sync.mutex_unlock(&ext.monitors_mu)
	return Monitor_Ref{id = id, target_base = target_base}
}

@(require_results)
monitor_actor :: proc(watcher: PID(Down_Event), target: ^Actor($State, $Msg)) -> Monitor_Ref {
	if target == nil {
		return {}
	}
	ext := ensure_actor_ext(&target.base)
	sync.mutex_lock(&ext.monitors_mu)
	if ext.drain_proc == nil {
		ext.drain_proc = proc(base: ^Actor_Base) {
			self := cast(^Actor(State, Msg))base
			actor_drain_mailbox(self)
		}
	}
	if ext.has_messages_proc == nil {
		ext.has_messages_proc = proc(base: ^Actor_Base) -> bool {
			self := cast(^Actor(State, Msg))base
			return !mpsc_is_empty(&self.mailbox)
		}
	}
	sync.mutex_unlock(&ext.monitors_mu)
	return monitor_impl(watcher, &target.base, 0)
}

@(require_results)
monitor_pid :: proc(watcher: PID(Down_Event), target: PID($Msg)) -> Monitor_Ref {
	if target.ref == nil {
		return {}
	}
	ext := ensure_actor_ext(&target.ref.base)
	sync.mutex_lock(&ext.monitors_mu)
	ensure_ref_callbacks(ext, Msg)
	sync.mutex_unlock(&ext.monitors_mu)
	return monitor_impl(watcher, &target.ref.base, target.generation)
}

monitor :: proc {
	monitor_actor,
	monitor_pid,
}

demonitor :: proc(ref: Monitor_Ref) {
	if ref.target_base == nil || ref.id == 0 {
		return
	}
	ext := get_actor_ext(ref.target_base)
	if ext == nil {
		return
	}
	sync.mutex_lock(&ext.monitors_mu)
	for i := 0; i < len(ext.monitors); i += 1 {
		if ext.monitors[i].id == ref.id {
			unordered_remove(&ext.monitors, i)
			break
		}
	}
	sync.mutex_unlock(&ext.monitors_mu)
}

@(private)
try_call_would_deadlock :: proc(s: ^Scheduler, caller_ctx: ^Context, target_base: ^Actor_Base) -> bool {
	if caller_ctx == nil || caller_ctx.current_actor == nil {
		return false
	}
	caller := caller_ctx.current_actor
	if caller == target_base {
		return true
	}
	if caller_ctx.sched == s && s != nil && s.num_workers == 1 && intrinsics.atomic_load_explicit(&target_base.in_step, .Seq_Cst) != 0 {
		return true
	}
	if _tls_call_depth >= MAX_COOPERATIVE_CALL_DEPTH {
		return true
	}
	cur := target_base
	for steps := 0; cur != nil && steps < MAX_COOPERATIVE_CALL_DEPTH; steps += 1 {
		ext := get_actor_ext(cur)
		if ext == nil {
			break
		}
		nxt := intrinsics.atomic_load_explicit(&ext.calling_target, .Acquire)
		if nxt == caller {
			return true
		}
		cur = nxt
	}
	return false
}

@(require_results)
try_call_sched_pid :: proc(
	s: ^Scheduler,
	target: PID($Msg),
	msg: Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	if target.ref == nil {
		return {}, .Target_Stopped
	}
	target_base := &target.ref.base
	if intrinsics.atomic_load_explicit(&target_base.status, .Acquire) == u32(Actor_Status.Stopped) {
		return {}, .Target_Stopped
	}
	if target.generation != 0 && intrinsics.atomic_load_explicit(&target_base.generation, .Acquire) != target.generation {
		return {}, .Stale_Generation
	}

	eff_sched := resolve_target_scheduler(s, target.ref)
	if eff_sched == nil {
		return {}, .Target_Stopped
	}

	caller_ctx := _tls_active_ctx
	if try_call_would_deadlock(eff_sched, caller_ctx, target_base) {
		return {}, .Deadlock_Detected
	}

	slot := Call_Slot(Res){
		ready    = sync.Futex(CALL_STATE_PENDING),
		res_type = typeid_of(Res),
	}
	env, hint := acquire_auto_envelope(eff_sched, target.ref, -1)
	env.flags |= ENVELOPE_FLAG_CALL
	env.reply_slot = &slot
	env.msg = msg

	// Double-check lifecycle before pushing into the mailbox.
	if intrinsics.atomic_load_explicit(&target_base.status, .Acquire) == u32(Actor_Status.Stopped) {
		env.reply_slot = nil
		env.flags &~= ENVELOPE_FLAG_CALL
		actor_recycle_single_envelope(target.ref, env)
		return {}, .Target_Stopped
	}
	if target.generation != 0 && intrinsics.atomic_load_explicit(&target_base.generation, .Acquire) != target.generation {
		env.reply_slot = nil
		env.flags &~= ENVELOPE_FLAG_CALL
		actor_recycle_single_envelope(target.ref, env)
		return {}, .Stale_Generation
	}

	var_caller_ext: ^Actor_Extension
	if caller_ctx != nil && caller_ctx.current_actor != nil {
		var_caller_ext = ensure_actor_ext(caller_ctx.current_actor)
		if var_caller_ext != nil {
			intrinsics.atomic_store_explicit(&var_caller_ext.calling_target, target_base, .Release)
		}
	}
	defer if var_caller_ext != nil {
		intrinsics.atomic_store_explicit(&var_caller_ext.calling_target, nil, .Release)
	}

	mpsc_push(&target.ref.mailbox, env)
	scheduler_schedule_actor(eff_sched, target_base, hint)

	if intrinsics.atomic_load_explicit(&target_base.status, .Seq_Cst) == u32(Actor_Status.Stopped) &&
	   intrinsics.atomic_load_explicit((^u32)(&slot.ready), .Acquire) < CALL_STATE_REPLIED {
		actor_drain_stopped_target(target_base, caller_ctx == nil)
	}

	final_state := wait_for_call_slot(eff_sched, &slot, caller_ctx)
	if final_state == CALL_STATE_REPLIED {
		return slot.result, .None
	}
	return {}, .Call_Target_Crashed
}

@(require_results)
try_call_sched_actor :: proc(
	s: ^Scheduler,
	target: ^Actor($State, $Msg),
	msg: Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	if target == nil {
		return {}, .Target_Stopped
	}
	return try_call_sched_pid(s, PID(Msg){ref = &target.ref, generation = 0}, msg, Res)
}

@(require_results)
try_call_ambient_pid :: proc(
	target: PID($Msg),
	msg: Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	if target.ref == nil {
		return {}, .Target_Stopped
	}
	s := resolve_target_scheduler(nil, target.ref)
	return try_call_sched_pid(s, target, msg, Res)
}

@(require_results)
try_call_ambient_actor :: proc(
	target: ^Actor($State, $Msg),
	msg: Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	if target == nil {
		return {}, .Target_Stopped
	}
	s := resolve_target_scheduler(nil, &target.ref)
	return try_call_sched_pid(s, PID(Msg){ref = &target.ref, generation = 0}, msg, Res)
}

try_call :: proc {
	try_call_sched_pid,
	try_call_sched_actor,
	try_call_ambient_pid,
	try_call_ambient_actor,
}

// -----------------------------------------------------------------------------
// OTP Supervisor, Child Lifecycle & Administration
// -----------------------------------------------------------------------------

Restart_Strategy :: enum u8 {
	One_For_One  = 0,
	One_For_All  = 1,
	Rest_For_One = 2,
}

Restart_Policy :: enum u8 {
	Permanent = 0,
	Transient = 1,
	Temporary = 2,
}

Clock_Proc :: #type proc(user_data: rawptr) -> time.Tick

Supervisor_Error :: enum u8 {
	None                  = 0,
	Not_Found             = 1,
	Child_Still_Running   = 2,
	Child_Already_Present = 3,
	Max_Children_Reached  = 4,
	Supervisor_Stopping   = 5,
}

Child_Counts :: struct {
	specs:       int,
	active:      int,
	supervisors: int,
	workers:     int,
}

Child_Info :: struct {
	id:            u64,
	base:          ^Actor_Base,
	status:        Actor_Status,
	policy:        Restart_Policy,
	is_supervisor: bool,
	generation:    u32,
	restarts:      int,
}

Child_Hooks :: struct($State: typeid) {
	on_reinit:    proc(state: ^State, arena_alloc: mem.Allocator),
	on_stop:      proc(state: ^State, reason: Exit_Reason),
	use_arena:    bool,
	keep_mailbox: bool,
}

Supervisor_Config :: struct {
	strategy:      Restart_Strategy,
	max_restarts:  int,
	max_window:    time.Duration,
	within_window: time.Duration,
	max_children:  int,
	clock:         proc() -> time.Tick,
	now_proc:      Clock_Proc,
	clock_data:    rawptr,
	id:            u32,
}

Supervisor_Stats :: struct {
	restart_cycles:     int,
	children_restarted: int,
	total_restarts:     int,
	escalated:          bool,
	last_fault:         Fault_Info,
}

Child_Down :: struct {
	child_id:   u64,
	generation: u32,
	reason:     Exit_Reason,
	fault:      Fault_Info,
}

Supervisor_Msg :: union {
	Child_Down,
}

Supervisor_State :: struct {
	cfg:   Supervisor_Config,
	stats: Supervisor_Stats,
}

Supervisor :: Actor(Supervisor_State, Supervisor_Msg)

SUP_RUNNING  :: u32(0)
SUP_STOPPING :: u32(1)
SUP_STOPPED  :: u32(2)

@(private)
Child_Entry :: struct {
	id:            u64,
	base:          ^Actor_Base,
	policy:        Restart_Policy,
	is_supervisor: bool,
	expected_gen:  u32,
	restarts:      int,
	admin_stopped: bool,
}

@(private)
Supervisor_Internal :: struct {
	mu:              sync.Mutex,
	op_mu:           sync.Recursive_Mutex,
	sup_actor:       ^Supervisor,
	children:        [dynamic]Child_Entry,
	restart_history: [dynamic]time.Tick,
	next_child_id:   u64,
	sup_state:       u32,
}

@(private)
get_sup_internal :: #force_inline proc(sup: ^Supervisor) -> ^Supervisor_Internal {
	if sup == nil {
		return nil
	}
	ext := get_actor_ext(&sup.base)
	if ext == nil {
		return nil
	}
	return ext.sup_internal
}

@(private)
supervisor_now :: #force_inline proc(cfg: ^Supervisor_Config) -> time.Tick {
	if cfg.now_proc != nil {
		return cfg.now_proc(cfg.clock_data)
	}
	if cfg.clock != nil {
		return cfg.clock()
	}
	return time.tick_now()
}

@(private)
should_restart_for_reason :: #force_inline proc(policy: Restart_Policy, reason: Exit_Reason) -> bool {
	switch policy {
	case .Permanent:
		return true
	case .Transient:
		return reason == .Panic || reason == .Error_Return || reason == .Escalated
	case .Temporary:
		return false
	}
	return false
}

@(private)
run_trapped_stop_hook :: #force_no_inline proc(ext: ^Actor_Extension, base: ^Actor_Base, reason: Exit_Reason) {
	if ext == nil || ext.stop_proc == nil {
		return
	}
	frame: Trap_Frame = ---
	frame.active       = true
	frame.fault        = {}
	frame.hook_count   = 0
	frame.prev         = _tls_trap_frame
	frame.saved_alloc  = context.allocator
	frame.saved_assert = context.assertion_failure_proc
	frame.arena_data   = nil
	turn_alloc         := context.allocator
	if ext.use_arena && ext.arena_initialized {
		turn_alloc       = ext.arena_alloc
		frame.arena_data = ext.arena_alloc.data
	}
	context.allocator              = turn_alloc
	context.assertion_failure_proc = actor_assertion_failure_proc
	_tls_trap_frame = &frame

	when ODIN_OS == .Windows {
		code := trap_setjmp_win(&frame.jmp, nil)
	} else {
		code := trap_setjmp_posix(&frame.jmp)
	}

	if code == 0 {
		ext.stop_proc(base, ext.user_on_stop, reason)
		_tls_trap_frame                = frame.prev
		context.assertion_failure_proc = frame.saved_assert
		context.allocator              = frame.saved_alloc
		return
	}

	_tls_trap_frame                = frame.prev
	context.assertion_failure_proc = frame.saved_assert
	context.allocator              = frame.saved_alloc
	if code != 2 {
		run_abort_hooks(&frame)
	}
}

@(private)
run_trapped_reinit_hook :: #force_no_inline proc(ext: ^Actor_Extension, base: ^Actor_Base) -> (ok: bool, fault: Fault_Info) {
	if ext == nil || ext.reinit_proc == nil {
		return true, {}
	}
	frame: Trap_Frame = ---
	frame.active       = true
	frame.fault        = {}
	frame.hook_count   = 0
	frame.prev         = _tls_trap_frame
	frame.saved_alloc  = context.allocator
	frame.saved_assert = context.assertion_failure_proc
	frame.arena_data   = nil
	alloc_arg          := ext.allocator
	turn_alloc         := context.allocator
	if ext.use_arena && ext.arena_initialized {
		turn_alloc       = ext.arena_alloc
		frame.arena_data = ext.arena_alloc.data
		alloc_arg        = ext.arena_alloc
	}
	context.allocator              = turn_alloc
	context.assertion_failure_proc = actor_assertion_failure_proc
	_tls_trap_frame = &frame

	when ODIN_OS == .Windows {
		code := trap_setjmp_win(&frame.jmp, nil)
	} else {
		code := trap_setjmp_posix(&frame.jmp)
	}

	if code == 0 {
		ext.reinit_proc(base, ext.user_on_reinit, alloc_arg)
		_tls_trap_frame                = frame.prev
		context.assertion_failure_proc = frame.saved_assert
		context.allocator              = frame.saved_alloc
		return true, {}
	}

	_tls_trap_frame                = frame.prev
	context.assertion_failure_proc = frame.saved_assert
	context.allocator              = frame.saved_alloc
	if code != 2 {
		run_abort_hooks(&frame)
	}
	return false, frame.fault
}

@(private)
prepare_child_reset_unlocked :: proc(c: ^Actor_Base) {
	if c == nil {
		return
	}
	is_self_step := _tls_active_ctx != nil && _tls_active_ctx.current_actor == c
	if !is_self_step {
		for intrinsics.atomic_load_explicit(&c.in_step, .Seq_Cst) != 0 {
			intrinsics.cpu_relax()
		}
	}

	cext := get_actor_ext(c)
	if cext != nil {
		sync.mutex_lock(&cext.monitors_mu)
		intrinsics.atomic_store_explicit(&cext.finalized, 0, .Release)
		intrinsics.atomic_store_explicit(&cext.stopped_drained, 0, .Release)
		if !cext.keep_mailbox && cext.drain_proc != nil {
			cext.drain_proc(c)
		}
		sync.mutex_unlock(&cext.monitors_mu)
		if cext.use_arena && cext.arena_initialized {
			vmem.arena_free_all(&cext.arena)
		}
		if cext.init_state_copy != nil && cext.state_size > 0 {
			dst := rawptr(uintptr(c) + cext.state_offset)
			mem.copy(dst, cext.init_state_copy, cext.state_size)
		}
	}
}

@(private)
activate_restarted_child :: proc(c: ^Actor_Base) {
	intrinsics.atomic_store_explicit(&c.status, u32(Actor_Status.Running), .Seq_Cst)
	cext := get_actor_ext(c)
	if cext != nil && cext.has_messages_proc != nil && cext.has_messages_proc(c) {
		s := c.reg_sched
		if s != nil {
			scheduler_schedule_actor(s, c)
		}
	}
}

@(private)
stop_live_child_for_restart_unlocked :: proc(c: ^Actor_Base, policy: Restart_Policy, is_supervisor: bool, admin_stopped: bool) {
	if admin_stopped || c == nil {
		return
	}
	if is_supervisor {
		child_sup := cast(^Supervisor)c
		perm := policy == .Temporary
		if !perm {
			intrinsics.atomic_store_explicit(&child_sup.base.status, u32(Actor_Status.Faulted), .Seq_Cst)
		}
		stop_supervisor_subtree_iterative(child_sup, .Supervisor_Shutdown, -1, {}, permanent_stop = perm)
		return
	}
	target_status := u32(Actor_Status.Stopped) if policy == .Temporary else u32(Actor_Status.Faulted)
	prev := intrinsics.atomic_exchange_explicit(&c.status, target_status, .Seq_Cst)
	is_self_step := _tls_active_ctx != nil && _tls_active_ctx.current_actor == c
	if !is_self_step {
		for intrinsics.atomic_load_explicit(&c.in_step, .Seq_Cst) != 0 {
			intrinsics.cpu_relax()
		}
	}
	cext := get_actor_ext(c)
	if prev == u32(Actor_Status.Running) {
		run_trapped_stop_hook(cext, c, .Supervisor_Shutdown)
	}
	if policy == .Temporary {
		finalize_actor_stopped(c, .Supervisor_Shutdown, {})
		free_actor_reset_resources(cext)
	}
}

@(private)
Stop_Frame :: struct {
	sup:      ^Supervisor,
	next_idx: int,
	perm:     bool,
}

@(private)
stop_supervisor_subtree_iterative :: proc(
	root: ^Supervisor,
	reason: Exit_Reason,
	crashed_root_child_idx: int,
	crash_fault: Fault_Info,
	permanent_stop: bool,
	mark_stopped: bool = false,
) {
	if root == nil {
		return
	}
	root_si := get_sup_internal(root)
	if root_si == nil {
		if permanent_stop || mark_stopped {
			finalize_actor_stopped(&root.base, reason, crash_fault)
		}
		return
	}
	if intrinsics.atomic_load_explicit(&root_si.sup_state, .Acquire) == SUP_STOPPED {
		return
	}
	alloc := root.allocator

	// Phase 1: Iteratively mark non-stopped supervisors in the subtree STOPPING + Stopped/Faulted status
	// and wait for any active `step_proc` turn on those supervisors to quiesce before reading rosters.
	visit_stack := make([dynamic]^Supervisor, 0, 16, alloc)
	defer delete(visit_stack)
	append(&visit_stack, root)

	for len(visit_stack) > 0 {
		cur := pop(&visit_stack)
		csi := get_sup_internal(cur)
		if csi == nil {
			continue
		}
		was_already_stopped := false
		for {
			cur_st := intrinsics.atomic_load_explicit(&csi.sup_state, .Acquire)
			if cur_st == SUP_STOPPED {
				was_already_stopped = true
				break
			}
			if _, cas_ok := intrinsics.atomic_compare_exchange_weak_explicit(
				&csi.sup_state,
				cur_st,
				SUP_STOPPING,
				.Seq_Cst,
				.Acquire,
			); cas_ok {
				break
			}
		}
		if was_already_stopped && !permanent_stop {
			continue
		}
		if permanent_stop || mark_stopped {
			intrinsics.atomic_store_explicit(&cur.base.status, u32(Actor_Status.Stopped), .Seq_Cst)
		} else if cur != root {
			intrinsics.atomic_store_explicit(&cur.base.status, u32(Actor_Status.Faulted), .Seq_Cst)
		}
		if _tls_active_ctx == nil || _tls_active_ctx.current_actor != &cur.base {
			for intrinsics.atomic_load_explicit(&cur.base.in_step, .Seq_Cst) != 0 {
				intrinsics.cpu_relax()
			}
		}
		sync.mutex_lock(&csi.mu)
		for entry in csi.children {
			if !permanent_stop && entry.admin_stopped {
				continue
			}
			if entry.is_supervisor && entry.base != nil {
				append(&visit_stack, cast(^Supervisor)entry.base)
			}
		}
		sync.mutex_unlock(&csi.mu)
	}

	// Phase 2: Iteratively stop children in strict reverse insertion order without stack recursion.
	frames := make([dynamic]Stop_Frame, 0, 16, alloc)
	defer delete(frames)

	sync.mutex_lock(&root_si.mu)
	root_len := len(root_si.children)
	sync.mutex_unlock(&root_si.mu)
	append(&frames, Stop_Frame{sup = root, next_idx = root_len - 1, perm = permanent_stop})

	for len(frames) > 0 {
		top := &frames[len(frames) - 1]
		tsi := get_sup_internal(top.sup)
		if tsi == nil {
			pop(&frames)
			continue
		}
		cur_perm := top.perm
		sync.mutex_lock(&tsi.mu)
		idx := top.next_idx
		if idx < 0 || idx >= len(tsi.children) {
			if top.sup != root {
				// Clean temporary bystanders from inner supervisors if melting down under parent
				for i := len(tsi.children) - 1; i >= 0; i -= 1 {
					if tsi.children[i].policy == .Temporary {
						free_actor_reset_resources(get_actor_ext(tsi.children[i].base))
						ordered_remove(&tsi.children, i)
					}
				}
			}
			sync.mutex_unlock(&tsi.mu)
			done_sup := pop(&frames).sup
			if cur_perm || mark_stopped {
				if intrinsics.atomic_load_explicit(&tsi.sup_state, .Acquire) != SUP_STOPPED {
					intrinsics.atomic_store_explicit(&tsi.sup_state, SUP_STOPPED, .Seq_Cst)
				}
				sup_reason := Exit_Reason.Escalated if (done_sup == root && crashed_root_child_idx >= 0) else reason
				finalize_actor_stopped(&done_sup.base, sup_reason, crash_fault if done_sup == root else {})
			}
			continue
		}
		entry := tsi.children[idx]
		top.next_idx = idx - 1
		sync.mutex_unlock(&tsi.mu)

		if !cur_perm && entry.admin_stopped {
			continue
		}

		if entry.is_supervisor && entry.base != nil {
			child_sup := cast(^Supervisor)entry.base
			child_si := get_sup_internal(child_sup)
			c_len := 0
			if child_si != nil {
				sync.mutex_lock(&child_si.mu)
				c_len = len(child_si.children)
				sync.mutex_unlock(&child_si.mu)
			}
			child_perm := cur_perm || (entry.policy == .Temporary)
			append(&frames, Stop_Frame{sup = child_sup, next_idx = c_len - 1, perm = child_perm})
			continue
		}

		c := entry.base
		if c == nil {
			continue
		}
		target_status := u32(Actor_Status.Stopped) if (cur_perm || mark_stopped || entry.policy == .Temporary) else u32(Actor_Status.Faulted)
		prev := intrinsics.atomic_exchange_explicit(&c.status, target_status, .Seq_Cst)
		if _tls_active_ctx == nil || _tls_active_ctx.current_actor != c {
			for intrinsics.atomic_load_explicit(&c.in_step, .Seq_Cst) != 0 {
				intrinsics.cpu_relax()
			}
		}
		cext := get_actor_ext(c)
		is_crashed_child := (top.sup == root && idx == crashed_root_child_idx)
		if !is_crashed_child && prev == u32(Actor_Status.Running) && !entry.admin_stopped {
			run_trapped_stop_hook(cext, c, reason)
		}
		if cur_perm || mark_stopped || entry.policy == .Temporary {
			eff_reason := reason if !is_crashed_child else crash_fault.reason
			eff_fault  := crash_fault if is_crashed_child else Fault_Info{}
			finalize_actor_stopped(c, eff_reason, eff_fault)
			if cur_perm || entry.policy == .Temporary {
				free_actor_reset_resources(cext)
			}
		} else {
			if cext != nil {
				if !cext.keep_mailbox && cext.drain_proc != nil {
					sync.mutex_lock(&cext.monitors_mu)
					cext.drain_proc(c)
					sync.mutex_unlock(&cext.monitors_mu)
				}
				if cext.use_arena && cext.arena_initialized {
					vmem.arena_free_all(&cext.arena)
				}
			}
		}
	}
}

@(private)
supervisor_stop_as_child :: proc(base: ^Actor_Base, _fn_ptr: rawptr, reason: Exit_Reason) {
	child_sup := cast(^Supervisor)base
	stop_supervisor_subtree_iterative(child_sup, reason, -1, {}, permanent_stop = false)
}

@(private)
supervisor_reinit_as_child :: proc(base: ^Actor_Base, _fn_ptr: rawptr, _alloc: mem.Allocator) {
	root_sup := cast(^Supervisor)base
	root_si := get_sup_internal(root_sup)
	if root_si == nil {
		return
	}
	alloc := root_sup.allocator
	stack := make([dynamic]^Supervisor, 0, 16, alloc)
	defer delete(stack)
	append(&stack, root_sup)

	for len(stack) > 0 {
		cur := pop(&stack)
		csi := get_sup_internal(cur)
		if csi == nil {
			continue
		}
		sync.recursive_mutex_lock(&csi.op_mu)
		sync.mutex_lock(&csi.mu)
		clear(&csi.restart_history)
		intrinsics.atomic_store_explicit(&csi.sup_state, SUP_RUNNING, .Seq_Cst)

		for i := len(csi.children) - 1; i >= 0; i -= 1 {
			if csi.children[i].policy == .Temporary {
				free_actor_reset_resources(get_actor_ext(csi.children[i].base))
				ordered_remove(&csi.children, i)
			}
		}

		child_ids := make([]u64, len(csi.children), alloc)
		for entry, i in csi.children {
			child_ids[i] = entry.id
		}
		sync.mutex_unlock(&csi.mu)

		reinit_failed := false
		for cid in child_ids {
			sync.mutex_lock(&csi.mu)
			if intrinsics.atomic_load_explicit(&csi.sup_state, .Acquire) != SUP_RUNNING {
				sync.mutex_unlock(&csi.mu)
				reinit_failed = true
				break
			}
			idx := -1
			for e, i in csi.children {
				if e.id == cid {
					idx = i
					break
				}
			}
			if idx < 0 || csi.children[idx].admin_stopped {
				sync.mutex_unlock(&csi.mu)
				continue
			}
			entry := &csi.children[idx]
			if entry.is_supervisor && entry.base != nil {
				sub := cast(^Supervisor)entry.base
				intrinsics.atomic_add_explicit(&sub.base.generation, 1, .Acq_Rel)
				entry.expected_gen = intrinsics.atomic_load_explicit(&sub.base.generation, .Acquire)
				entry.restarts += 1
				cur.state.stats.children_restarted += 1
				cur.state.stats.total_restarts += 1
				append(&stack, sub)
				intrinsics.atomic_store_explicit(&sub.base.status, u32(Actor_Status.Running), .Seq_Cst)
				sync.mutex_unlock(&csi.mu)
				continue
			}
			c_base := entry.base
			sync.mutex_unlock(&csi.mu)

			prepare_child_reset_unlocked(c_base)

			sync.mutex_lock(&csi.mu)
			idx = -1
			for e, i in csi.children {
				if e.id == cid {
					idx = i
					break
				}
			}
			if idx < 0 || csi.children[idx].admin_stopped || intrinsics.atomic_load_explicit(&csi.sup_state, .Acquire) != SUP_RUNNING {
				sync.mutex_unlock(&csi.mu)
				continue
			}
			entry = &csi.children[idx]
			new_gen := intrinsics.atomic_add_explicit(&c_base.generation, 1, .Acq_Rel) + 1
			entry.expected_gen = new_gen
			entry.restarts += 1
			cur.state.stats.children_restarted += 1
			cur.state.stats.total_restarts += 1
			sync.mutex_unlock(&csi.mu)

			ok, _ := run_trapped_reinit_hook(get_actor_ext(c_base), c_base)
			if !ok {
				reinit_failed = true
				break
			}
			activate_restarted_child(c_base)
		}
		delete(child_ids, alloc)
		sync.recursive_mutex_unlock(&csi.op_mu)

		if reinit_failed {
			delete(stack)
			stack = nil
			stop_supervisor_subtree_iterative(root_sup, .Supervisor_Shutdown, -1, {}, permanent_stop = false)
			intrinsics.atomic_store_explicit(&root_sup.base.status, u32(Actor_Status.Faulted), .Seq_Cst)
			fail_turn_reason(.Panic, "child reinit failed during supervisor_reinit_as_child")
			return
		}
	}
}

@(private)
supervisor_handle_down :: proc(sup: ^Supervisor, down: Child_Down) {
	si := get_sup_internal(sup)
	if si == nil {
		return
	}

	sync.recursive_mutex_lock(&si.op_mu)
	defer sync.recursive_mutex_unlock(&si.op_mu)

	// Acquire supervisor roster lock cleanly.
	sync.mutex_lock(&si.mu)
	if intrinsics.atomic_load_explicit(&si.sup_state, .Acquire) != SUP_RUNNING {
		sync.mutex_unlock(&si.mu)
		return
	}

	idx := -1
	for i := 0; i < len(si.children); i += 1 {
		if si.children[i].id == down.child_id {
			idx = i
			break
		}
	}
	if idx < 0 {
		sync.mutex_unlock(&si.mu)
		return
	}

	entry := &si.children[idx]
	if entry.admin_stopped || down.generation != entry.expected_gen {
		sync.mutex_unlock(&si.mu)
		return
	}

	if !should_restart_for_reason(entry.policy, down.reason) {
		base_ptr := entry.base
		is_temp := entry.policy == .Temporary
		if !is_temp {
			entry.admin_stopped = true
		} else {
			ordered_remove(&si.children, idx)
		}
		sync.mutex_unlock(&si.mu)
		finalize_actor_stopped(base_ptr, down.reason, down.fault)
		if is_temp {
			free_actor_reset_resources(get_actor_ext(base_ptr))
		}
		return
	}

	sup.state.stats.last_fault = down.fault
	start_idx := idx
	end_idx := idx
	switch sup.state.cfg.strategy {
	case .One_For_One:
		start_idx = idx
		end_idx = idx
	case .Rest_For_One:
		start_idx = idx
		end_idx = len(si.children) - 1
	case .One_For_All:
		start_idx = 0
		end_idx = len(si.children) - 1
	}

	cohort_ids := make([]u64, end_idx - start_idx + 1, sup.allocator)
	for i := start_idx; i <= end_idx; i += 1 {
		cohort_ids[i - start_idx] = si.children[i].id
	}
	defer delete(cohort_ids, sup.allocator)

	window := sup.state.cfg.max_window
	if window <= 0 {
		window = sup.state.cfg.within_window
	}
	if window <= 0 {
		window = 5 * time.Second
	}

	siblings_stopped := false
	cur_fault := down.fault
	cur_fault.reason = down.reason
	reinit_bases := make([dynamic]^Actor_Base, 0, len(cohort_ids), sup.allocator)
	defer delete(reinit_bases)

	for {
		if intrinsics.atomic_load_explicit(&si.sup_state, .Acquire) != SUP_RUNNING {
			sync.mutex_unlock(&si.mu)
			return
		}

		// Sliding intensity window check
		now := supervisor_now(&sup.state.cfg)
		write_i := 0
		for r_i := 0; r_i < len(si.restart_history); r_i += 1 {
			if time.tick_diff(si.restart_history[r_i], now) <= window {
				si.restart_history[write_i] = si.restart_history[r_i]
				write_i += 1
			}
		}
		resize(&si.restart_history, write_i)

		if len(si.restart_history) >= sup.state.cfg.max_restarts {
			cur_crash_idx := -1
			for e, i in si.children {
				if e.id == down.child_id {
					cur_crash_idx = i
					break
				}
			}
			sup.state.stats.escalated = true
			sup_ext := get_actor_ext(&sup.base)
			has_parent := sup_ext != nil && sup_ext.is_supervised && sup_ext.sup != nil

			if has_parent {
				// Release lock while iterative subtree helper traverses sub-supervisors
				sync.mutex_unlock(&si.mu)
				stop_supervisor_subtree_iterative(sup, .Supervisor_Shutdown, cur_crash_idx, cur_fault, permanent_stop = false)
				// Prune temporary children from our roster while retaining Permanent/Transient for parent restart
				sync.mutex_lock(&si.mu)
				for i := len(si.children) - 1; i >= 0; i -= 1 {
					if si.children[i].policy == .Temporary {
						free_actor_reset_resources(get_actor_ext(si.children[i].base))
						ordered_remove(&si.children, i)
					}
				}
				sync.mutex_unlock(&si.mu)

				intrinsics.atomic_store_explicit(&sup.base.status, u32(Actor_Status.Faulted), .Seq_Cst)
				s := resolve_target_scheduler(nil, &sup_ext.sup.ref)
				if s != nil {
					gen := intrinsics.atomic_load_explicit(&sup.base.generation, .Acquire)
					send_sched_auto(s, sup_ext.sup, Supervisor_Msg(Child_Down{
						child_id   = sup_ext.child_id,
						generation = gen,
						reason     = .Escalated,
						fault      = cur_fault,
					}))
				}
			} else {
				sync.mutex_unlock(&si.mu)
				stop_supervisor_subtree_iterative(sup, .Supervisor_Shutdown, cur_crash_idx, cur_fault, permanent_stop = true)
			}
			return
		}

		append(&si.restart_history, now)
		sup.state.stats.restart_cycles += 1

		// Stop affected live siblings in reverse order on first iteration without holding si.mu
		if !siblings_stopped {
			siblings_stopped = true
			sync.mutex_unlock(&si.mu)
			for k := len(cohort_ids) - 1; k >= 0; k -= 1 {
				sib_id := cohort_ids[k]
				if sib_id == down.child_id {
					continue
				}
				sync.mutex_lock(&si.mu)
				sib_base: ^Actor_Base
				sib_policy: Restart_Policy
				sib_is_sup: bool
				sib_admin_stopped := true
				for e in si.children {
					if e.id == sib_id {
						sib_base          = e.base
						sib_policy        = e.policy
						sib_is_sup        = e.is_supervisor
						sib_admin_stopped = e.admin_stopped
						break
					}
				}
				sync.mutex_unlock(&si.mu)
				stop_live_child_for_restart_unlocked(sib_base, sib_policy, sib_is_sup, sib_admin_stopped)
			}
			sync.mutex_lock(&si.mu)
			if intrinsics.atomic_load_explicit(&si.sup_state, .Acquire) != SUP_RUNNING {
				sync.mutex_unlock(&si.mu)
				return
			}
		}

		// Reinitialize cohort in forward order without holding si.mu across in_step waits or hooks
		clear(&reinit_bases)
		reinit_failed := false
		for cid in cohort_ids {
			c_idx := -1
			for e, i in si.children {
				if e.id == cid {
					c_idx = i
					break
				}
			}
			if c_idx < 0 {
				continue
			}
			c_entry := &si.children[c_idx]
			if c_entry.admin_stopped || c_entry.policy == .Temporary {
				continue
			}
			c_base := c_entry.base
			sync.mutex_unlock(&si.mu)

			prepare_child_reset_unlocked(c_base)

			sync.mutex_lock(&si.mu)
			if intrinsics.atomic_load_explicit(&si.sup_state, .Acquire) != SUP_RUNNING {
				sync.mutex_unlock(&si.mu)
				return
			}
			c_idx = -1
			for e, i in si.children {
				if e.id == cid {
					c_idx = i
					break
				}
			}
			if c_idx < 0 || si.children[c_idx].admin_stopped || si.children[c_idx].policy == .Temporary {
				continue
			}
			c_entry = &si.children[c_idx]
			new_gen := intrinsics.atomic_add_explicit(&c_base.generation, 1, .Acq_Rel) + 1
			c_entry.expected_gen = new_gen
			c_entry.restarts += 1
			sup.state.stats.children_restarted += 1
			sup.state.stats.total_restarts += 1
			sync.mutex_unlock(&si.mu)

			ok, rfault := run_trapped_reinit_hook(get_actor_ext(c_base), c_base)

			sync.mutex_lock(&si.mu)
			if !ok {
				reinit_failed = true
				cur_fault = rfault
				cur_fault.reason = .Panic
				break
			}
			append(&reinit_bases, c_base)
		}

		if reinit_failed {
			sync.mutex_unlock(&si.mu)
			// Roll back already-reinitialized cohort children in reverse order via trapped `on_stop`
			for i := len(reinit_bases) - 1; i >= 0; i -= 1 {
				rb_base := reinit_bases[i]
				run_trapped_stop_hook(get_actor_ext(rb_base), rb_base, .Supervisor_Shutdown)
			}
			sync.mutex_lock(&si.mu)
			continue
		}

		// Cohort reinitialized cleanly: collect children to activate and prune Temporary bystanders
		clear(&reinit_bases)
		for cid in cohort_ids {
			for i := len(si.children) - 1; i >= 0; i -= 1 {
				if si.children[i].id != cid {
					continue
				}
				if si.children[i].policy == .Temporary {
					free_actor_reset_resources(get_actor_ext(si.children[i].base))
					ordered_remove(&si.children, i)
				} else if !si.children[i].admin_stopped {
					append(&reinit_bases, si.children[i].base)
				}
				break
			}
		}
		sync.mutex_unlock(&si.mu)
		for c_base in reinit_bases {
			activate_restarted_child(c_base)
		}
		return
	}
}

@(private)
supervisor_handle_msg :: proc(ctx: ^Context, self: ^Supervisor, msg: ^Supervisor_Msg) {
	_ = ctx
	switch m in msg^ {
	case Child_Down:
		supervisor_handle_down(self, m)
	}
}

@(require_results)
spawn_supervisor_sched_cfg :: proc(
	s: ^Scheduler,
	cfg: Supervisor_Config = {},
	allocator := context.allocator,
) -> ^Supervisor {
	eff_sched := s if s != nil else current_scheduler()
	eff_alloc := resolve_spawn_allocator(eff_sched, allocator)
	norm_cfg := cfg
	if norm_cfg.max_window <= 0 {
		if norm_cfg.within_window > 0 {
			norm_cfg.max_window = norm_cfg.within_window
		} else {
			norm_cfg.max_window = 5 * time.Second
		}
	}
	init_state := Supervisor_State{cfg = norm_cfg}
	sup, err := spawn(eff_sched, init_state, supervisor_handle_msg, norm_cfg.id, eff_alloc)
	if err != .None || sup == nil {
		return nil
	}
	ext := ensure_actor_ext(&sup.base, eff_alloc)
	si := new(Supervisor_Internal, ext.allocator)
	si.sup_actor       = sup
	si.children        = make([dynamic]Child_Entry, 0, 8, ext.allocator)
	si.restart_history = make([dynamic]time.Tick, 0, 8, ext.allocator)
	si.sup_state       = SUP_RUNNING
	ext.sup_internal   = si
	ext.drain_proc = proc(base: ^Actor_Base) {
		self := cast(^Supervisor)base
		actor_drain_mailbox(self)
	}
	ext.has_messages_proc = proc(base: ^Actor_Base) -> bool {
		self := cast(^Supervisor)base
		return !mpsc_is_empty(&self.mailbox)
	}
	return sup
}

@(require_results)
spawn_supervisor_cfg :: proc(
	cfg: Supervisor_Config = {},
	allocator := context.allocator,
) -> ^Supervisor {
	return spawn_supervisor_sched_cfg(current_scheduler(), cfg, allocator)
}

spawn_supervisor :: proc {
	spawn_supervisor_sched_cfg,
	spawn_supervisor_cfg,
}

enable_actor_supervision :: proc(
	a: ^Actor($State, $Msg),
	initial_state: State,
	on_reinit: proc(state: ^State, arena_alloc: mem.Allocator) = nil,
	use_arena: bool = true,
	keep_mailbox: bool = false,
) -> mem.Allocator_Error {
	if a == nil {
		return .Invalid_Pointer
	}
	ext := ensure_actor_ext(&a.base, a.allocator)
	a.base.trap_panics = true

	if ext.init_state_copy == nil && size_of(State) > 0 {
		ext.init_state_copy = mem.alloc(size_of(State), align_of(State), ext.allocator) or_return
	}
	if size_of(State) > 0 {
		copy_src := initial_state
		mem.copy(ext.init_state_copy, &copy_src, size_of(State))
	}
	ext.state_offset   = offset_of(Actor(State, Msg), state)
	ext.state_size     = size_of(State)
	ext.use_arena      = use_arena
	ext.keep_mailbox   = keep_mailbox
	ext.user_on_reinit = rawptr(on_reinit)
	ext.reinit_proc = proc(base: ^Actor_Base, fn_ptr: rawptr, arena_alloc: mem.Allocator) {
		self := cast(^Actor(State, Msg))base
		if fn_ptr != nil {
			fn := cast(proc(state: ^State, arena_alloc: mem.Allocator))fn_ptr
			fn(&self.state, arena_alloc)
		}
	}
	ext.drain_proc = proc(base: ^Actor_Base) {
		self := cast(^Actor(State, Msg))base
		actor_drain_mailbox(self)
	}
	ext.has_messages_proc = proc(base: ^Actor_Base) -> bool {
		self := cast(^Actor(State, Msg))base
		return !mpsc_is_empty(&self.mailbox)
	}
	if use_arena && !ext.arena_initialized {
		vmem.arena_init_growing(&ext.arena, uint(16 * mem.Kilobyte)) or_return
		ext.arena_alloc = vmem.arena_allocator(&ext.arena)
		ext.arena_initialized = true
	}
	return .None
}

supervise_hooks :: proc(
	sup: ^Supervisor,
	child: ^Actor($State, $Msg),
	initial_state: State,
	policy: Restart_Policy,
	hooks: Child_Hooks(State),
	child_id: u64 = 0,
) -> Supervisor_Error {
	si := get_sup_internal(sup)
	if si == nil || child == nil {
		return .Supervisor_Stopping
	}

	sync.mutex_lock(&si.mu)
	defer sync.mutex_unlock(&si.mu)

	if intrinsics.atomic_load_explicit(&si.sup_state, .Acquire) != SUP_RUNNING {
		return .Supervisor_Stopping
	}
	if sup.state.cfg.max_children > 0 && len(si.children) >= sup.state.cfg.max_children {
		return .Max_Children_Reached
	}

	eff_id := child_id
	if eff_id != 0 {
		for entry in si.children {
			if entry.id == eff_id {
				return .Child_Already_Present
			}
		}
		if eff_id >= si.next_child_id {
			si.next_child_id = eff_id
		}
	} else {
		si.next_child_id += 1
		eff_id = si.next_child_id
	}

	ext := ensure_actor_ext(&child.base, sup.allocator)
	child.base.trap_panics = true
	if size_of(State) > 0 {
		if ext.init_state_copy == nil {
			init_raw, alloc_err := mem.alloc(size_of(State), align_of(State), ext.allocator)
			assert(alloc_err == nil)
			ext.init_state_copy = init_raw
		}
		copy_src := initial_state
		mem.copy(ext.init_state_copy, &copy_src, size_of(State))
	}
	ext.is_supervised  = true
	ext.sup            = sup
	ext.child_id       = eff_id
	ext.state_offset   = offset_of(Actor(State, Msg), state)
	ext.state_size     = size_of(State)
	ext.use_arena      = hooks.use_arena
	ext.keep_mailbox   = hooks.keep_mailbox
	ext.user_on_reinit = rawptr(hooks.on_reinit)
	ext.user_on_stop   = rawptr(hooks.on_stop)
	ext.reinit_proc = proc(base: ^Actor_Base, fn_ptr: rawptr, arena_alloc: mem.Allocator) {
		self := cast(^Actor(State, Msg))base
		if fn_ptr != nil {
			fn := cast(proc(state: ^State, arena_alloc: mem.Allocator))fn_ptr
			fn(&self.state, arena_alloc)
		}
	}
	ext.stop_proc = proc(base: ^Actor_Base, fn_ptr: rawptr, reason: Exit_Reason) {
		self := cast(^Actor(State, Msg))base
		if fn_ptr != nil {
			fn := cast(proc(state: ^State, reason: Exit_Reason))fn_ptr
			fn(&self.state, reason)
		}
	}
	ext.drain_proc = proc(base: ^Actor_Base) {
		self := cast(^Actor(State, Msg))base
		actor_drain_mailbox(self)
	}
	ext.has_messages_proc = proc(base: ^Actor_Base) -> bool {
		self := cast(^Actor(State, Msg))base
		return !mpsc_is_empty(&self.mailbox)
	}
	if hooks.use_arena && !ext.arena_initialized {
		if arena_err := vmem.arena_init_growing(&ext.arena, uint(16 * mem.Kilobyte)); arena_err == nil {
			ext.arena_alloc = vmem.arena_allocator(&ext.arena)
			ext.arena_initialized = true
		}
	}

	gen := intrinsics.atomic_load_explicit(&child.base.generation, .Acquire)
	append(&si.children, Child_Entry{
		id            = eff_id,
		base          = &child.base,
		policy        = policy,
		is_supervisor = false,
		expected_gen  = gen,
		restarts      = 0,
		admin_stopped = false,
	})
	return .None
}

supervise_fn :: proc(
	sup: ^Supervisor,
	child: ^Actor($State, $Msg),
	initial_state: State,
	policy: Restart_Policy = .Permanent,
	on_reinit: proc(state: ^State, arena_alloc: mem.Allocator) = nil,
	on_stop: proc(state: ^State, reason: Exit_Reason) = nil,
	use_arena: bool = true,
	keep_mailbox: bool = false,
	child_id: u64 = 0,
) -> Supervisor_Error {
	return supervise_hooks(
		sup,
		child,
		initial_state,
		policy,
		Child_Hooks(State){
			on_reinit    = on_reinit,
			on_stop      = on_stop,
			use_arena    = use_arena,
			keep_mailbox = keep_mailbox,
		},
		child_id,
	)
}

supervise :: proc {
	supervise_hooks,
	supervise_fn,
}

supervise_supervisor :: proc(
	parent: ^Supervisor,
	child_sup: ^Supervisor,
	policy: Restart_Policy = .Permanent,
	child_id: u64 = 0,
) -> Supervisor_Error {
	psi := get_sup_internal(parent)
	csi := get_sup_internal(child_sup)
	if psi == nil || csi == nil {
		return .Supervisor_Stopping
	}

	sync.mutex_lock(&psi.mu)
	defer sync.mutex_unlock(&psi.mu)

	if intrinsics.atomic_load_explicit(&psi.sup_state, .Acquire) != SUP_RUNNING {
		return .Supervisor_Stopping
	}
	if parent.state.cfg.max_children > 0 && len(psi.children) >= parent.state.cfg.max_children {
		return .Max_Children_Reached
	}

	eff_id := child_id
	if eff_id != 0 {
		for entry in psi.children {
			if entry.id == eff_id {
				return .Child_Already_Present
			}
		}
		if eff_id >= psi.next_child_id {
			psi.next_child_id = eff_id
		}
	} else {
		psi.next_child_id += 1
		eff_id = psi.next_child_id
	}

	ext := ensure_actor_ext(&child_sup.base, parent.allocator)
	child_sup.base.trap_panics = true
	ext.is_supervised = true
	ext.sup           = parent
	ext.child_id      = eff_id
	ext.use_arena     = false
	ext.keep_mailbox  = false
	ext.reinit_proc   = supervisor_reinit_as_child
	ext.stop_proc     = supervisor_stop_as_child

	gen := intrinsics.atomic_load_explicit(&child_sup.base.generation, .Acquire)
	append(&psi.children, Child_Entry{
		id            = eff_id,
		base          = &child_sup.base,
		policy        = policy,
		is_supervisor = true,
		expected_gen  = gen,
		restarts      = 0,
		admin_stopped = false,
	})
	return .None
}

@(require_results)
supervisor_stats :: proc(sup: ^Supervisor) -> Supervisor_Stats {
	if sup == nil {
		return {}
	}
	si := get_sup_internal(sup)
	if si == nil {
		return sup.state.stats
	}
	sync.mutex_lock(&si.mu)
	stats := sup.state.stats
	sync.mutex_unlock(&si.mu)
	return stats
}

stop_supervisor :: proc(sup: ^Supervisor) {
	stop_supervisor_subtree_iterative(sup, .Supervisor_Shutdown, -1, {}, permanent_stop = true)
}

@(private)
free_subtree_reset_resources :: proc(root_base: ^Actor_Base, alloc: mem.Allocator) {
	if root_base == nil {
		return
	}
	stack := make([dynamic]^Actor_Base, 0, 8, alloc)
	defer delete(stack)
	append(&stack, root_base)
	for len(stack) > 0 {
		cur := pop(&stack)
		cext := get_actor_ext(cur)
		if cext != nil && cext.sup_internal != nil {
			csi := cext.sup_internal
			sync.mutex_lock(&csi.mu)
			for entry in csi.children {
				if entry.base != nil {
					append(&stack, entry.base)
				}
			}
			sync.mutex_unlock(&csi.mu)
		}
		free_actor_reset_resources(cext)
	}
}

supervisor_terminate_child :: proc(sup: ^Supervisor, child_id: u64) -> Supervisor_Error {
	si := get_sup_internal(sup)
	if si == nil {
		return .Not_Found
	}

	sync.recursive_mutex_lock(&si.op_mu)
	defer sync.recursive_mutex_unlock(&si.op_mu)

	sync.mutex_lock(&si.mu)
	idx := -1
	for i := 0; i < len(si.children); i += 1 {
		if si.children[i].id == child_id {
			idx = i
			break
		}
	}
	if idx < 0 {
		sync.mutex_unlock(&si.mu)
		return .Not_Found
	}

	entry := &si.children[idx]
	already_admin_stopped := entry.admin_stopped
	is_sup := entry.is_supervisor
	policy := entry.policy
	c := entry.base
	if !already_admin_stopped {
		entry.admin_stopped = true
		sync.mutex_unlock(&si.mu)

		if is_sup && c != nil {
			child_sup := cast(^Supervisor)c
			stop_supervisor_subtree_iterative(
				child_sup,
				.Supervisor_Shutdown,
				-1,
				{},
				permanent_stop = (policy == .Temporary),
				mark_stopped = (policy != .Temporary),
			)
		} else if c != nil {
			prev := intrinsics.atomic_exchange_explicit(&c.status, u32(Actor_Status.Stopped), .Seq_Cst)
			if _tls_active_ctx == nil || _tls_active_ctx.current_actor != c {
				for intrinsics.atomic_load_explicit(&c.in_step, .Seq_Cst) != 0 {
					intrinsics.cpu_relax()
				}
			}
			cext := get_actor_ext(c)
			if prev == u32(Actor_Status.Running) {
				run_trapped_stop_hook(cext, c, .Supervisor_Shutdown)
			}
			finalize_actor_stopped(c, .Supervisor_Shutdown, {})
		}

		sync.mutex_lock(&si.mu)
		idx = -1
		for i := 0; i < len(si.children); i += 1 {
			if si.children[i].id == child_id {
				idx = i
				break
			}
		}
	}

	if idx >= 0 && si.children[idx].policy == .Temporary {
		free_subtree_reset_resources(si.children[idx].base, sup.allocator)
		ordered_remove(&si.children, idx)
	}
	sync.mutex_unlock(&si.mu)
	return .None
}

supervisor_restart_child :: proc(sup: ^Supervisor, child_id: u64) -> Supervisor_Error {
	si := get_sup_internal(sup)
	if si == nil {
		return .Not_Found
	}

	sync.recursive_mutex_lock(&si.op_mu)
	defer sync.recursive_mutex_unlock(&si.op_mu)

	sync.mutex_lock(&si.mu)
	if intrinsics.atomic_load_explicit(&si.sup_state, .Acquire) != SUP_RUNNING {
		sync.mutex_unlock(&si.mu)
		return .Supervisor_Stopping
	}

	idx := -1
	for i := 0; i < len(si.children); i += 1 {
		if si.children[i].id == child_id {
			idx = i
			break
		}
	}
	if idx < 0 {
		sync.mutex_unlock(&si.mu)
		return .Not_Found
	}

	entry := &si.children[idx]
	cur_status := intrinsics.atomic_load_explicit(&entry.base.status, .Acquire)
	if !entry.admin_stopped && cur_status != u32(Actor_Status.Stopped) {
		sync.mutex_unlock(&si.mu)
		return .Child_Still_Running
	}

	c_base := entry.base
	was_admin_stopped := entry.admin_stopped
	sync.mutex_unlock(&si.mu)

	cext := get_actor_ext(c_base)
	prev_finalized: u32 = 0
	prev_stopped_drained: u32 = 0
	if cext != nil {
		sync.mutex_lock(&cext.monitors_mu)
		prev_finalized = cext.finalized
		prev_stopped_drained = cext.stopped_drained
		sync.mutex_unlock(&cext.monitors_mu)
	}

	prepare_child_reset_unlocked(c_base)

	sync.mutex_lock(&si.mu)
	if intrinsics.atomic_load_explicit(&si.sup_state, .Acquire) != SUP_RUNNING {
		sync.mutex_unlock(&si.mu)
		return .Supervisor_Stopping
	}
	idx = -1
	for i := 0; i < len(si.children); i += 1 {
		if si.children[i].id == child_id {
			idx = i
			break
		}
	}
	if idx < 0 {
		sync.mutex_unlock(&si.mu)
		return .Not_Found
	}
	entry = &si.children[idx]
	new_gen := intrinsics.atomic_add_explicit(&c_base.generation, 1, .Acq_Rel) + 1
	entry.expected_gen = new_gen
	entry.restarts += 1
	sup.state.stats.children_restarted += 1
	sup.state.stats.total_restarts += 1
	sync.mutex_unlock(&si.mu)

	ok, _ := run_trapped_reinit_hook(cext, c_base)
	if !ok {
		if cext != nil && was_admin_stopped {
			sync.mutex_lock(&cext.monitors_mu)
			intrinsics.atomic_store_explicit(&cext.finalized, prev_finalized, .Release)
			intrinsics.atomic_store_explicit(&cext.stopped_drained, prev_stopped_drained, .Release)
			sync.mutex_unlock(&cext.monitors_mu)
		}
		return .Supervisor_Stopping
	}

	sync.mutex_lock(&si.mu)
	for i := 0; i < len(si.children); i += 1 {
		if si.children[i].id == child_id {
			si.children[i].admin_stopped = false
			break
		}
	}
	sync.mutex_unlock(&si.mu)

	activate_restarted_child(c_base)
	return .None
}

supervisor_delete_child :: proc(sup: ^Supervisor, child_id: u64) -> Supervisor_Error {
	si := get_sup_internal(sup)
	if si == nil {
		return .Not_Found
	}

	sync.mutex_lock(&si.mu)
	defer sync.mutex_unlock(&si.mu)

	idx := -1
	for i := 0; i < len(si.children); i += 1 {
		if si.children[i].id == child_id {
			idx = i
			break
		}
	}
	if idx < 0 {
		return .Not_Found
	}

	entry := &si.children[idx]
	cur_status := intrinsics.atomic_load_explicit(&entry.base.status, .Acquire)
	if !entry.admin_stopped && cur_status != u32(Actor_Status.Stopped) {
		return .Child_Still_Running
	}

	free_subtree_reset_resources(entry.base, sup.allocator)
	ordered_remove(&si.children, idx)
	return .None
}

@(require_results)
supervisor_count_children :: proc(sup: ^Supervisor) -> Child_Counts {
	si := get_sup_internal(sup)
	if si == nil {
		return {}
	}
	sync.mutex_lock(&si.mu)
	defer sync.mutex_unlock(&si.mu)

	counts := Child_Counts{
		specs = len(si.children),
	}
	for entry in si.children {
		st := Actor_Status(intrinsics.atomic_load_explicit(&entry.base.status, .Acquire))
		if !entry.admin_stopped && st == .Running {
			counts.active += 1
			if entry.is_supervisor {
				counts.supervisors += 1
			} else {
				counts.workers += 1
			}
		}
	}
	return counts
}

@(require_results)
supervisor_which_children :: proc(sup: ^Supervisor, allocator := context.allocator) -> []Child_Info {
	si := get_sup_internal(sup)
	if si == nil {
		return nil
	}
	sync.mutex_lock(&si.mu)
	defer sync.mutex_unlock(&si.mu)

	res := make([]Child_Info, len(si.children), allocator)
	for entry, i in si.children {
		res[i] = Child_Info{
			id            = entry.id,
			base          = entry.base,
			status        = Actor_Status(intrinsics.atomic_load_explicit(&entry.base.status, .Acquire)),
			policy        = entry.policy,
			is_supervisor = entry.is_supervisor,
			generation    = intrinsics.atomic_load_explicit(&entry.base.generation, .Acquire),
			restarts      = entry.restarts,
		}
	}
	return res
}

// -----------------------------------------------------------------------------
// Live Actor Reclamation (`actor_base_destroy`), Named `Registry($Key)` &
// Sharded Multi-Tenant `Partitioned_Supervisor`
// -----------------------------------------------------------------------------

actor_base_destroy_sched :: proc(s: ^Scheduler, base: ^Actor_Base) {
	if base == nil {
		return
	}
	if ext := get_actor_ext(base); ext != nil {
		if ext.sup_internal != nil {
			if intrinsics.atomic_load_explicit(&ext.sup_internal.sup_state, .Acquire) != SUP_STOPPED {
				stop_supervisor_subtree_iterative(cast(^Supervisor)base, .Supervisor_Shutdown, -1, {}, permanent_stop = true)
			} else {
				finalize_actor_stopped(base, .Supervisor_Shutdown, {})
			}
		} else {
			finalize_actor_stopped(base, .Supervisor_Shutdown, {})
		}
	} else {
		intrinsics.atomic_store_explicit(&base.status, u32(Actor_Status.Stopped), .Seq_Cst)
	}

	eff_s := s if s != nil else base.reg_sched
	if eff_s != nil && eff_s.started && intrinsics.atomic_load_explicit(&eff_s.running, .Acquire) {
		if _tls_active_ctx == nil || _tls_active_ctx.current_actor != base {
			for intrinsics.atomic_load_explicit(&base.scheduled, .Acquire) != 0 ||
			    intrinsics.atomic_load_explicit(&base.in_step, .Seq_Cst) != 0 {
				intrinsics.cpu_relax()
			}
		}
	}

	if reg_s := base.reg_sched; reg_s != nil {
		sync.mutex_lock(&reg_s.registry_mu)
		for i := 0; i < len(reg_s.owned_actors); i += 1 {
			if reg_s.owned_actors[i] == base {
				unordered_remove(&reg_s.owned_actors, i)
				break
			}
		}
		sync.mutex_unlock(&reg_s.registry_mu)
		base.reg_sched = nil
	}

	if base.destroy_proc != nil {
		destroy_fn := base.destroy_proc
		base.destroy_proc = nil
		destroy_fn(base)
	}
}

actor_base_destroy_single :: proc(base: ^Actor_Base) {
	actor_base_destroy_sched(nil, base)
}

actor_base_destroy :: proc {
	actor_base_destroy_single,
	actor_base_destroy_sched,
}

Registry_Entry :: struct {
	ref:      ^Actor_Base,
	msg_type: typeid,
}

Registry :: struct($Key: typeid) {
	mu:        sync.RW_Mutex,
	entries:   map[Key]Registry_Entry,
	allocator: mem.Allocator,
}

default_registry: Registry(u64)

registry_init :: proc(r: ^Registry($Key), allocator := context.allocator) {
	if r == nil {
		return
	}
	r.allocator = allocator
	r.entries   = make(map[Key]Registry_Entry, 16, allocator)
}

@(private)
detach_registry_hook_from_actor :: proc(base: ^Actor_Base, reg_ptr: rawptr) {
	if base == nil || reg_ptr == nil {
		return
	}
	ext := get_actor_ext(base)
	if ext == nil {
		return
	}
	sync.mutex_lock(&ext.monitors_mu)
	for i := len(ext.reg_hooks) - 1; i >= 0; i -= 1 {
		if ext.reg_hooks[i].reg_ptr == reg_ptr {
			unordered_remove(&ext.reg_hooks, i)
		}
	}
	sync.mutex_unlock(&ext.monitors_mu)
}

registry_destroy :: proc(r: ^Registry($Key)) {
	if r == nil {
		return
	}
	sync.rw_mutex_lock(&r.mu)
	if r.entries != nil {
		for _, entry in r.entries {
			detach_registry_hook_from_actor(entry.ref, rawptr(r))
		}
		delete(r.entries)
		r.entries = nil
	}
	sync.rw_mutex_unlock(&r.mu)
}

@(private)
registry_cleanup_for_key :: proc($Key: typeid) -> proc(reg_ptr: rawptr, base: ^Actor_Base) {
	return proc(reg_ptr: rawptr, base: ^Actor_Base) {
		r := cast(^Registry(Key))reg_ptr
		if r == nil || base == nil {
			return
		}
		sync.rw_mutex_lock(&r.mu)
		defer sync.rw_mutex_unlock(&r.mu)
		if r.entries == nil {
			return
		}
		for {
			found_key: Key
			has_match := false
			for k, v in r.entries {
				if v.ref == base {
					found_key = k
					has_match = true
					break
				}
			}
			if !has_match {
				break
			}
			delete_key(&r.entries, found_key)
		}
	}
}

@(private)
ensure_ref_callbacks :: proc(ext: ^Actor_Extension, $Msg: typeid) {
	if ext == nil {
		return
	}
	if ext.drain_proc == nil {
		ext.drain_proc = proc(base: ^Actor_Base) {
			ref := cast(^Actor_Ref(Msg))base
			actor_ref_drain_mailbox(ref)
		}
	}
	if ext.has_messages_proc == nil {
		ext.has_messages_proc = proc(base: ^Actor_Base) -> bool {
			ref := cast(^Actor_Ref(Msg))base
			return !mpsc_is_empty(&ref.mailbox)
		}
	}
}

@(private)
registry_register_raw :: proc(
	r: ^Registry($Key),
	key: Key,
	base: ^Actor_Base,
	msg_type: typeid,
	expected_gen: u32 = 0,
) -> bool {
	if r == nil || base == nil {
		return false
	}
	if intrinsics.atomic_load_explicit(&base.status, .Acquire) == u32(Actor_Status.Stopped) {
		return false
	}
	if expected_gen != 0 && intrinsics.atomic_load_explicit(&base.generation, .Acquire) != expected_gen {
		return false
	}

	sync.rw_mutex_lock(&r.mu)
	if r.entries == nil {
		alloc := r.allocator if r.allocator.procedure != nil else resolve_ext_allocator(base, context.allocator)
		r.allocator = alloc
		r.entries   = make(map[Key]Registry_Entry, 16, alloc)
	}
	if existing, found := r.entries[key]; found && existing.ref != nil {
		if existing.ref != base &&
		   intrinsics.atomic_load_explicit(&existing.ref.status, .Acquire) != u32(Actor_Status.Stopped) {
			sync.rw_mutex_unlock(&r.mu)
			return false
		}
	}
	r.entries[key] = Registry_Entry{
		ref      = base,
		msg_type = msg_type,
	}
	sync.rw_mutex_unlock(&r.mu)

	ext := ensure_actor_ext(base)
	cleanup_fn := registry_cleanup_for_key(Key)
	sync.mutex_lock(&ext.monitors_mu)
	if ext.finalized != 0 || intrinsics.atomic_load_explicit(&base.status, .Acquire) == u32(Actor_Status.Stopped) {
		sync.mutex_unlock(&ext.monitors_mu)
		cleanup_fn(rawptr(r), base)
		return false
	}
	hook_present := false
	for h in ext.reg_hooks {
		if h.reg_ptr == rawptr(r) {
			hook_present = true
			break
		}
	}
	if !hook_present {
		append(&ext.reg_hooks, Registry_Cleanup_Hook{
			reg_ptr      = rawptr(r),
			cleanup_proc = cleanup_fn,
		})
	}
	sync.mutex_unlock(&ext.monitors_mu)
	return true
}

registry_register_actor :: proc(r: ^Registry($Key), key: Key, a: ^Actor($State, $Msg)) -> bool {
	if a == nil {
		return false
	}
	ext := ensure_actor_ext(&a.base)
	sync.mutex_lock(&ext.monitors_mu)
	ensure_ref_callbacks(ext, Msg)
	sync.mutex_unlock(&ext.monitors_mu)
	return registry_register_raw(r, key, &a.base, typeid_of(Msg), 0)
}

registry_register_pid :: proc(r: ^Registry($Key), key: Key, p: PID($Msg)) -> bool {
	if p.ref == nil {
		return false
	}
	ext := ensure_actor_ext(&p.ref.base)
	sync.mutex_lock(&ext.monitors_mu)
	ensure_ref_callbacks(ext, Msg)
	sync.mutex_unlock(&ext.monitors_mu)
	return registry_register_raw(r, key, &p.ref.base, typeid_of(Msg), p.generation)
}

registry_register :: proc {
	registry_register_actor,
	registry_register_pid,
}

registry_unregister :: proc(r: ^Registry($Key), key: Key) {
	if r == nil {
		return
	}
	sync.rw_mutex_lock(&r.mu)
	if r.entries != nil {
		if old_entry, found := r.entries[key]; found {
			delete_key(&r.entries, key)
			still_registered := false
			for _, v in r.entries {
				if v.ref == old_entry.ref {
					still_registered = true
					break
				}
			}
			if !still_registered {
				detach_registry_hook_from_actor(old_entry.ref, rawptr(r))
			}
		}
	}
	sync.rw_mutex_unlock(&r.mu)
}

@(require_results)
registry_lookup :: proc(r: ^Registry($Key), key: Key, $Msg: typeid) -> (PID(Msg), bool) {
	if r == nil {
		return {}, false
	}
	sync.rw_mutex_shared_lock(&r.mu)
	if r.entries == nil {
		sync.rw_mutex_shared_unlock(&r.mu)
		return {}, false
	}
	entry, found := r.entries[key]
	if !found || entry.ref == nil || entry.msg_type != typeid_of(Msg) {
		sync.rw_mutex_shared_unlock(&r.mu)
		return {}, false
	}
	st  := Actor_Status(intrinsics.atomic_load_explicit(&entry.ref.status, .Acquire))
	gen := intrinsics.atomic_load_explicit(&entry.ref.generation, .Acquire)
	sync.rw_mutex_shared_unlock(&r.mu)

	if st == .Stopped {
		return {}, false
	}
	return PID(Msg){ref = cast(^Actor_Ref(Msg))entry.ref, generation = gen}, true
}

register_name_actor :: proc(key: u64, a: ^Actor($State, $Msg)) -> bool {
	return registry_register_actor(&default_registry, key, a)
}

register_name_pid :: proc(key: u64, p: PID($Msg)) -> bool {
	return registry_register_pid(&default_registry, key, p)
}

register_name :: proc {
	register_name_actor,
	register_name_pid,
}

unregister_name :: proc(key: u64) {
	registry_unregister(&default_registry, key)
}

@(require_results)
whereis_name :: proc(key: u64, $Msg: typeid) -> (PID(Msg), bool) {
	return registry_lookup(&default_registry, key, Msg)
}

DEFAULT_PARTITIONS  :: 64
MAX_TENANT_CHILDREN :: 8

Tenant_Subtree :: struct {
	sup:      ^Supervisor,
	router:   ^Actor_Base,
	msg_type: typeid,
	children: [dynamic; MAX_TENANT_CHILDREN]^Actor_Base,
	user_ctx: rawptr,
}

Tenant_Factory_Proc :: #type proc(sched: ^Scheduler, org_id: u64, user_data: rawptr) -> Tenant_Subtree
Tenant_Evict_Proc   :: #type proc(org_id: u64, subtree: ^Tenant_Subtree, user_data: rawptr)

Partitioned_Supervisor_Config :: struct {
	num_partitions: int,
	max_children:   int,
	idle_timeout:   time.Duration,
	factory:        Tenant_Factory_Proc,
	on_evict:       Tenant_Evict_Proc,
	user_data:      rawptr,
	registry:       ^Registry(u64),
	clock:          proc() -> time.Tick,
	now_proc:       Clock_Proc,
	clock_data:     rawptr,
}

@(private)
Tenant_Entry :: struct {
	org_id:         u64,
	subtree:        Tenant_Subtree,
	last_active_ns: i64,
	in_flight:      i32,
	evicting:       bool,
}

@(private)
Tenant_Partition :: struct #align(64) {
	mu:      sync.RW_Mutex,
	tenants: map[u64]^Tenant_Entry,
}

Partitioned_Supervisor :: struct {
	sched:          ^Scheduler,
	cfg:            Partitioned_Supervisor_Config,
	partitions:     []Tenant_Partition,
	active_tenants: int,
	evicted_count:  int,
	allocator:      mem.Allocator,
}

Dynamic_Supervisor :: Partitioned_Supervisor

@(private)
partitioned_supervisor_now :: #force_inline proc(ps: ^Partitioned_Supervisor) -> time.Tick {
	if ps.cfg.now_proc != nil {
		return ps.cfg.now_proc(ps.cfg.clock_data)
	}
	if ps.cfg.clock != nil {
		return ps.cfg.clock()
	}
	return time.tick_now()
}

@(private)
partition_index :: #force_inline proc(ps: ^Partitioned_Supervisor, org_id: u64) -> int {
	n := len(ps.partitions)
	if n <= 0 {
		return 0
	}
	return int(org_id % u64(n))
}

@(private)
actor_base_is_busy :: #force_inline proc(b: ^Actor_Base) -> bool {
	if b == nil {
		return false
	}
	return intrinsics.atomic_load_explicit(&b.scheduled, .Acquire) != 0 ||
	       intrinsics.atomic_load_explicit(&b.in_step, .Seq_Cst) != 0
}

@(private)
tenant_entry_is_busy :: proc(entry: ^Tenant_Entry) -> bool {
	if entry == nil {
		return false
	}
	if intrinsics.atomic_load_explicit(&entry.in_flight, .Acquire) > 0 {
		return true
	}
	if actor_base_is_busy(entry.subtree.router) {
		return true
	}
	for c in entry.subtree.children {
		if actor_base_is_busy(c) {
			return true
		}
	}
	if entry.subtree.sup != nil {
		if actor_base_is_busy(&entry.subtree.sup.base) {
			return true
		}
		if si := get_sup_internal(entry.subtree.sup); si != nil {
			sync.mutex_lock(&si.mu)
			for ce in si.children {
				if actor_base_is_busy(ce.base) {
					sync.mutex_unlock(&si.mu)
					return true
				}
			}
			sync.mutex_unlock(&si.mu)
		}
	}
	return false
}

@(private)
tenant_entry_is_dead :: proc(entry: ^Tenant_Entry) -> bool {
	if entry == nil || entry.evicting {
		return false
	}
	if intrinsics.atomic_load_explicit(&entry.in_flight, .Acquire) > 0 {
		return false
	}
	router_stopped := entry.subtree.router == nil ||
		intrinsics.atomic_load_explicit(&entry.subtree.router.status, .Acquire) == u32(Actor_Status.Stopped)
	if !router_stopped {
		return false
	}
	if entry.subtree.sup == nil {
		return true
	}
	if intrinsics.atomic_load_explicit(&entry.subtree.sup.base.status, .Acquire) == u32(Actor_Status.Stopped) {
		return true
	}
	return supervisor_count_children(entry.subtree.sup).active == 0
}

@(private)
detach_actor_from_scheduler :: proc(s: ^Scheduler, base: ^Actor_Base) {
	if s == nil || base == nil {
		return
	}
	for i := 0; i < len(s.owned_actors); i += 1 {
		if s.owned_actors[i] == base {
			unordered_remove(&s.owned_actors, i)
			return
		}
	}
}

@(private)
detach_tenant_from_scheduler :: proc(s: ^Scheduler, subtree: ^Tenant_Subtree) {
	if s == nil || subtree == nil {
		return
	}
	sync.mutex_lock(&s.registry_mu)
	if subtree.sup != nil {
		detach_actor_from_scheduler(s, &subtree.sup.base)
		if si := get_sup_internal(subtree.sup); si != nil {
			sync.mutex_lock(&si.mu)
			for ce in si.children {
				detach_actor_from_scheduler(s, ce.base)
			}
			sync.mutex_unlock(&si.mu)
		}
	}
	detach_actor_from_scheduler(s, subtree.router)
	for c in subtree.children {
		detach_actor_from_scheduler(s, c)
	}
	sync.mutex_unlock(&s.registry_mu)
}

@(private)
already_in_ptr_list :: proc(list: ^[dynamic]^Actor_Base, ptr: ^Actor_Base) -> bool {
	for p in list^ {
		if p == ptr {
			return true
		}
	}
	return false
}

@(private)
destroy_tenant_entry :: proc(ps: ^Partitioned_Supervisor, entry: ^Tenant_Entry) {
	if entry == nil {
		return
	}
	extra_children: [dynamic]^Actor_Base
	defer delete(extra_children)

	if entry.subtree.sup != nil {
		if si := get_sup_internal(entry.subtree.sup); si != nil {
			sync.mutex_lock(&si.mu)
			extra_children = make([dynamic]^Actor_Base, 0, len(si.children), ps.allocator)
			for ce in si.children {
				if ce.base != nil {
					append(&extra_children, ce.base)
				}
			}
			sync.mutex_unlock(&si.mu)
		}
		stop_supervisor(entry.subtree.sup)
	}

	destroyed := make([dynamic]^Actor_Base, 0, len(entry.subtree.children) + len(extra_children) + 2, ps.allocator)
	defer delete(destroyed)

	for i := len(entry.subtree.children) - 1; i >= 0; i -= 1 {
		c := entry.subtree.children[i]
		if c != nil && !already_in_ptr_list(&destroyed, c) {
			append(&destroyed, c)
			actor_base_destroy(ps.sched, c)
		}
	}
	for i := len(extra_children) - 1; i >= 0; i -= 1 {
		c := extra_children[i]
		if c != nil && !already_in_ptr_list(&destroyed, c) {
			append(&destroyed, c)
			actor_base_destroy(ps.sched, c)
		}
	}
	if entry.subtree.router != nil && !already_in_ptr_list(&destroyed, entry.subtree.router) {
		append(&destroyed, entry.subtree.router)
		actor_base_destroy(ps.sched, entry.subtree.router)
	}
	if entry.subtree.sup != nil && !already_in_ptr_list(&destroyed, &entry.subtree.sup.base) {
		append(&destroyed, &entry.subtree.sup.base)
		actor_base_destroy(ps.sched, &entry.subtree.sup.base)
	}
	free(entry, ps.allocator)
}

@(private)
cleanup_tenant_registry_entry :: proc(ps: ^Partitioned_Supervisor, entry: ^Tenant_Entry) {
	if ps == nil || ps.cfg.registry == nil || entry == nil {
		return
	}
	if entry.subtree.router != nil {
		registry_cleanup_for_key(u64)(rawptr(ps.cfg.registry), entry.subtree.router)
	}
}

@(private)
try_reserve_tenant_slot :: proc(ps: ^Partitioned_Supervisor) -> bool {
	if ps.cfg.max_children <= 0 {
		intrinsics.atomic_add_explicit(&ps.active_tenants, 1, .Acq_Rel)
		return true
	}
	for {
		cur := intrinsics.atomic_load_explicit(&ps.active_tenants, .Acquire)
		if cur >= ps.cfg.max_children {
			return false
		}
		if _, ok := intrinsics.atomic_compare_exchange_weak_explicit(
			&ps.active_tenants,
			cur,
			cur + 1,
			.Acq_Rel,
			.Acquire,
		); ok {
			return true
		}
	}
}

partitioned_supervisor_init :: proc(
	ps: ^Partitioned_Supervisor,
	s: ^Scheduler,
	cfg: Partitioned_Supervisor_Config,
	allocator := context.allocator,
) {
	eff_sched := s if s != nil else current_scheduler()
	eff_alloc := resolve_spawn_allocator(eff_sched, allocator)
	num_parts := cfg.num_partitions if cfg.num_partitions > 0 else DEFAULT_PARTITIONS
	ps.sched          = eff_sched
	ps.cfg            = cfg
	ps.partitions     = make([]Tenant_Partition, num_parts, eff_alloc)
	for i := 0; i < num_parts; i += 1 {
		ps.partitions[i].tenants = make(map[u64]^Tenant_Entry, 8, eff_alloc)
	}
	ps.active_tenants = 0
	ps.evicted_count  = 0
	ps.allocator      = eff_alloc
}

partitioned_supervisor_destroy :: proc(ps: ^Partitioned_Supervisor) {
	if ps == nil || ps.partitions == nil {
		return
	}
	for i := 0; i < len(ps.partitions); i += 1 {
		part := &ps.partitions[i]
		sync.rw_mutex_lock(&part.mu)
		entries := make([dynamic]^Tenant_Entry, 0, len(part.tenants), ps.allocator)
		for _, entry in part.tenants {
			if entry != nil {
				entry.evicting = true
				append(&entries, entry)
			}
		}
		delete(part.tenants)
		part.tenants = nil
		sync.rw_mutex_unlock(&part.mu)

		for entry in entries {
			cleanup_tenant_registry_entry(ps, entry)
			destroy_tenant_entry(ps, entry)
		}
		delete(entries)
	}
	delete(ps.partitions, ps.allocator)
	ps.partitions = nil
	intrinsics.atomic_store_explicit(&ps.active_tenants, 0, .Release)
}

@(private)
reap_dead_on_partition_locked :: proc(
	ps: ^Partitioned_Supervisor,
	part: ^Tenant_Partition,
	reap_list: ^[dynamic]^Tenant_Entry,
) {
	if part.tenants == nil {
		return
	}
	for _, entry in part.tenants {
		if entry != nil && !entry.evicting && tenant_entry_is_dead(entry) && !tenant_entry_is_busy(entry) {
			entry.evicting = true
			append(reap_list, entry)
		}
	}
	for e in reap_list^ {
		delete_key(&part.tenants, e.org_id)
	}
	if len(reap_list^) > 0 {
		intrinsics.atomic_sub_explicit(&ps.active_tenants, len(reap_list^), .Acq_Rel)
	}
}

@(private, require_results)
acquire_tenant_entry :: proc(
	ps: ^Partitioned_Supervisor,
	org_id: u64,
	factory_proc: Tenant_Factory_Proc,
	user_ctx: rawptr,
	inc_in_flight: bool,
) -> (^Tenant_Entry, Supervisor_Error) {
	if ps == nil || ps.partitions == nil {
		return nil, .Supervisor_Stopping
	}
	now  := partitioned_supervisor_now(ps)
	part := &ps.partitions[partition_index(ps, org_id)]

	// Fast path under shared lock
	sync.rw_mutex_shared_lock(&part.mu)
	if part.tenants != nil {
		if entry, found := part.tenants[org_id]; found && entry != nil && !entry.evicting && !tenant_entry_is_dead(entry) {
			intrinsics.atomic_store_explicit(&entry.last_active_ns, now._nsec, .Release)
			if inc_in_flight {
				intrinsics.atomic_add_explicit(&entry.in_flight, 1, .Acq_Rel)
			}
			sync.rw_mutex_shared_unlock(&part.mu)
			return entry, .None
		}
	}
	sync.rw_mutex_shared_unlock(&part.mu)

	// Slow path under exclusive partition lock
	reap_list := make([dynamic]^Tenant_Entry, 0, 4, ps.allocator)
	defer {
		for dead in reap_list {
			cleanup_tenant_registry_entry(ps, dead)
			destroy_tenant_entry(ps, dead)
		}
		delete(reap_list)
	}

	sync.rw_mutex_lock(&part.mu)
	if part.tenants == nil {
		sync.rw_mutex_unlock(&part.mu)
		return nil, .Supervisor_Stopping
	}
	if existing, found := part.tenants[org_id]; found && existing != nil {
		if !existing.evicting && !tenant_entry_is_dead(existing) {
			intrinsics.atomic_store_explicit(&existing.last_active_ns, now._nsec, .Release)
			if inc_in_flight {
				intrinsics.atomic_add_explicit(&existing.in_flight, 1, .Acq_Rel)
			}
			sync.rw_mutex_unlock(&part.mu)
			return existing, .None
		}
	}

	reap_dead_on_partition_locked(ps, part, &reap_list)

	if !try_reserve_tenant_slot(ps) {
		sync.rw_mutex_unlock(&part.mu)
		return nil, .Max_Children_Reached
	}

	eff_factory := factory_proc if factory_proc != nil else ps.cfg.factory
	if eff_factory == nil {
		intrinsics.atomic_sub_explicit(&ps.active_tenants, 1, .Acq_Rel)
		sync.rw_mutex_unlock(&part.mu)
		return nil, .Supervisor_Stopping
	}
	eff_ctx := user_ctx if user_ctx != nil else ps.cfg.user_data
	subtree := eff_factory(ps.sched, org_id, eff_ctx)
	detach_tenant_from_scheduler(ps.sched, &subtree)

	entry := new(Tenant_Entry, ps.allocator)
	entry^ = Tenant_Entry{
		org_id         = org_id,
		subtree        = subtree,
		last_active_ns = now._nsec,
		in_flight      = 1 if inc_in_flight else 0,
		evicting       = false,
	}
	part.tenants[org_id] = entry
	if ps.cfg.registry != nil && subtree.router != nil {
		_ = registry_register_raw(ps.cfg.registry, org_id, subtree.router, subtree.msg_type, 0)
	}
	sync.rw_mutex_unlock(&part.mu)
	return entry, .None
}

@(require_results)
get_or_spawn_tenant_cfg :: proc(
	ps: ^Partitioned_Supervisor,
	org_id: u64,
	$Msg: typeid,
) -> (PID(Msg), Supervisor_Error) {
	entry, err := acquire_tenant_entry(ps, org_id, nil, nil, inc_in_flight = true)
	if err != .None || entry == nil {
		return {}, err if err != .None else .Supervisor_Stopping
	}
	defer intrinsics.atomic_sub_explicit(&entry.in_flight, 1, .Acq_Rel)
	if entry.subtree.router == nil || entry.subtree.msg_type != typeid_of(Msg) {
		return {}, .Supervisor_Stopping
	}
	gen := intrinsics.atomic_load_explicit(&entry.subtree.router.generation, .Acquire)
	return PID(Msg){ref = cast(^Actor_Ref(Msg))entry.subtree.router, generation = gen}, .None
}

@(require_results)
get_or_spawn_tenant_with_factory :: proc(
	ps: ^Partitioned_Supervisor,
	org_id: u64,
	$Msg: typeid,
	factory_proc: Tenant_Factory_Proc,
	user_ctx: rawptr = nil,
) -> (PID(Msg), Supervisor_Error) {
	entry, err := acquire_tenant_entry(ps, org_id, factory_proc, user_ctx, inc_in_flight = true)
	if err != .None || entry == nil {
		return {}, err if err != .None else .Supervisor_Stopping
	}
	defer intrinsics.atomic_sub_explicit(&entry.in_flight, 1, .Acq_Rel)
	if entry.subtree.router == nil || entry.subtree.msg_type != typeid_of(Msg) {
		return {}, .Supervisor_Stopping
	}
	gen := intrinsics.atomic_load_explicit(&entry.subtree.router.generation, .Acquire)
	return PID(Msg){ref = cast(^Actor_Ref(Msg))entry.subtree.router, generation = gen}, .None
}

get_or_spawn_tenant :: proc {
	get_or_spawn_tenant_cfg,
	get_or_spawn_tenant_with_factory,
}

send_to_tenant :: proc(ps: ^Partitioned_Supervisor, org_id: u64, msg: $Msg) {
	entry, err := acquire_tenant_entry(ps, org_id, nil, nil, inc_in_flight = true)
	if err != .None || entry == nil {
		return
	}
	defer intrinsics.atomic_sub_explicit(&entry.in_flight, 1, .Acq_Rel)
	router := entry.subtree.router
	if router == nil || entry.subtree.msg_type != typeid_of(Msg) {
		return
	}
	send_sched_auto_pid(ps.sched, PID(Msg){ref = cast(^Actor_Ref(Msg))router, generation = 0}, msg)
}

@(require_results)
try_call_tenant :: proc(
	ps: ^Partitioned_Supervisor,
	org_id: u64,
	msg: $Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	entry, sup_err := acquire_tenant_entry(ps, org_id, nil, nil, inc_in_flight = true)
	if sup_err != .None || entry == nil {
		return {}, .Target_Stopped
	}
	defer intrinsics.atomic_sub_explicit(&entry.in_flight, 1, .Acq_Rel)
	router := entry.subtree.router
	if router == nil || entry.subtree.msg_type != typeid_of(Msg) {
		return {}, .Target_Stopped
	}
	return try_call_sched_pid(ps.sched, PID(Msg){ref = cast(^Actor_Ref(Msg))router, generation = 0}, msg, Res)
}

@(require_results)
call_tenant :: proc(
	ps: ^Partitioned_Supervisor,
	org_id: u64,
	msg: $Msg,
	$Res: typeid,
) -> Res {
	res, _ := try_call_tenant(ps, org_id, msg, Res)
	return res
}

sweep_idle_tenants :: proc(ps: ^Partitioned_Supervisor, now_override: time.Tick = {}) -> int {
	if ps == nil || ps.partitions == nil {
		return 0
	}
	now := now_override if now_override._nsec != 0 else partitioned_supervisor_now(ps)
	total_evicted := 0

	for i := 0; i < len(ps.partitions); i += 1 {
		part := &ps.partitions[i]
		evict_list := make([dynamic]^Tenant_Entry, 0, 8, ps.allocator)

		// Phase 1: Scan under partition lock, collect eligible entries, and erase after iteration.
		sync.rw_mutex_lock(&part.mu)
		if part.tenants != nil {
			for _, entry in part.tenants {
				if entry == nil || entry.evicting {
					continue
				}
				if tenant_entry_is_busy(entry) {
					continue
				}
				last_tick := time.Tick{_nsec = intrinsics.atomic_load_explicit(&entry.last_active_ns, .Acquire)}
				idle_due  := ps.cfg.idle_timeout > 0 && time.tick_diff(last_tick, now) >= ps.cfg.idle_timeout
				if idle_due || tenant_entry_is_dead(entry) {
					entry.evicting = true
					append(&evict_list, entry)
				}
			}
			for e in evict_list {
				delete_key(&part.tenants, e.org_id)
			}
			if len(evict_list) > 0 {
				intrinsics.atomic_sub_explicit(&ps.active_tenants, len(evict_list), .Acq_Rel)
				intrinsics.atomic_add_explicit(&ps.evicted_count, len(evict_list), .Acq_Rel)
			}
		}
		sync.rw_mutex_unlock(&part.mu)

		// Phase 2: Invoke eviction hooks and destroy evicted subtrees outside the partition lock.
		for e in evict_list {
			cleanup_tenant_registry_entry(ps, e)
			if ps.cfg.on_evict != nil {
				ps.cfg.on_evict(e.org_id, &e.subtree, ps.cfg.user_data)
			}
			destroy_tenant_entry(ps, e)
		}
		total_evicted += len(evict_list)
		delete(evict_list)
	}

	return total_evicted
}

@(require_results)
tenant_count :: proc(ps: ^Partitioned_Supervisor) -> int {
	if ps == nil || ps.partitions == nil {
		return 0
	}
	live_count := 0
	saw_dead := false
	for i := 0; i < len(ps.partitions); i += 1 {
		part := &ps.partitions[i]
		sync.rw_mutex_shared_lock(&part.mu)
		if part.tenants != nil {
			for _, entry in part.tenants {
				if entry == nil || entry.evicting {
					continue
				}
				if tenant_entry_is_dead(entry) {
					saw_dead = true
					continue
				}
				live_count += 1
			}
		}
		sync.rw_mutex_shared_unlock(&part.mu)
	}
	if saw_dead {
		for i := 0; i < len(ps.partitions); i += 1 {
			part := &ps.partitions[i]
			reap_list := make([dynamic]^Tenant_Entry, 0, 4, ps.allocator)
			sync.rw_mutex_lock(&part.mu)
			reap_dead_on_partition_locked(ps, part, &reap_list)
			sync.rw_mutex_unlock(&part.mu)
			for dead in reap_list {
				cleanup_tenant_registry_entry(ps, dead)
				destroy_tenant_entry(ps, dead)
			}
			delete(reap_list)
		}
	}
	return live_count
}

dynamic_supervisor_init :: proc(
	ds: ^Dynamic_Supervisor,
	s: ^Scheduler,
	cfg: Partitioned_Supervisor_Config,
	allocator := context.allocator,
) {
	partitioned_supervisor_init(ds, s, cfg, allocator)
}

dynamic_supervisor_destroy :: proc(ds: ^Dynamic_Supervisor) {
	partitioned_supervisor_destroy(ds)
}

@(require_results)
dynamic_supervisor_get_or_spawn_tenant :: proc(
	ds: ^Dynamic_Supervisor,
	org_id: u64,
	$Msg: typeid,
) -> (PID(Msg), Supervisor_Error) {
	return get_or_spawn_tenant_cfg(ds, org_id, Msg)
}

dynamic_supervisor_send_to_tenant :: proc(ds: ^Dynamic_Supervisor, org_id: u64, msg: $Msg) {
	send_to_tenant(ds, org_id, msg)
}

@(require_results)
dynamic_supervisor_try_call_tenant :: proc(
	ds: ^Dynamic_Supervisor,
	org_id: u64,
	msg: $Msg,
	$Res: typeid,
) -> (res: Res, err: Call_Error) {
	return try_call_tenant(ds, org_id, msg, Res)
}

@(require_results)
dynamic_supervisor_call_tenant :: proc(
	ds: ^Dynamic_Supervisor,
	org_id: u64,
	msg: $Msg,
	$Res: typeid,
) -> Res {
	return call_tenant(ds, org_id, msg, Res)
}

dynamic_supervisor_sweep_idle_tenants :: proc(ds: ^Dynamic_Supervisor, now_override: time.Tick = {}) -> int {
	return sweep_idle_tenants(ds, now_override)
}

@(require_results)
dynamic_supervisor_count :: proc(ds: ^Dynamic_Supervisor) -> int {
	return tenant_count(ds)
}
