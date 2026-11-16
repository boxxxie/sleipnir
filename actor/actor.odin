package actor

import "base:intrinsics"
import "base:runtime"
import "core:mem"
import "core:os"
import "core:sync"
import "core:thread"
import "core:time"

// =============================================================================
// Reusable Pony-Style M:N Actor & Sharded Work-Stealing Scheduler
//
// Features:
//   - Heterogeneous `Scheduler`: Can run any number of distinct `Actor($State, $Msg)`
//     types concurrently on a shared pool of M worker threads.
//   - Per-worker run-queue shards with work-stealing across peer shards.
//   - Per-actor batch quantum (`batch_quota`, default 64) to amortize scheduling.
//   - Lock-free `.Seq_Cst` Dekker wake/unschedule protocol (zero lost wakeups).
//   - Optional adaptive 2-phase Futex parking (`enable_parking = true`) for 0% idle CPU.
//   - Zero-copy `iso` message forwarding (`forward`) + `snmalloc` batched pool recycling.
//   - Ergonomic DSL features: `scoped_scheduler` (`@(deferred_out)`), ambient thread-local
//     scheduler/worker context, automatic per-message-type `Envelope_Pool` management,
//     message-typed `PID($Msg)` handles, and 2-arg / 3-arg / value-union handlers.
// =============================================================================

DEFAULT_BATCH_QUOTA         :: 64
DEFAULT_FUTEX_SPIN_LIMIT    :: 1024
DEFAULT_AUTO_POOL_CAPACITY  :: 256
MAX_COOPERATIVE_CALL_DEPTH  :: 32

CALL_STATE_PENDING   :: u32(0)
CALL_STATE_PARKED    :: u32(1)
CALL_STATE_SIGNALING :: u32(2)
CALL_STATE_REPLIED   :: u32(3)
CALL_STATE_FAULTED   :: u32(4)

STEP_MODE_ACTOR  :: 0 // proc(ctx: ^Context, self: ^Actor(State, Msg), msg: ^Msg)
STEP_MODE_STATE  :: 1 // proc(ctx: ^Context, state: ^State, msg: ^Msg)
STEP_MODE_SIMPLE :: 2 // proc(state: ^State, msg: ^Msg)
STEP_MODE_VAL    :: 3 // proc(state: ^State, msg: Msg)

// Thread-local ambient pointers so `actor.send(pid, msg)`, `actor.forward(pid, msg)`,
// `actor.spawn(State{}, handler)`, and `actor.complete_token()` work without threading
// `&sched` or `ctx` through every call.
@(thread_local)
_tls_active_ctx: ^Context

@(thread_local)
_tls_active_sched: ^Scheduler

@(thread_local)
_tls_call_depth: int

// Count of live initialized Schedulers so `default_registry` is destroyed only when the last exits.
_active_scheduler_count: i32

// Returns the active `^Context` when called inside an actor handler on a worker thread,
// or `nil` when called from an external thread.
current_context :: #force_inline proc() -> ^Context {
	return _tls_active_ctx
}

// Returns the currently executing actor's lifecycle generation on worker threads, or `0`.
current_generation :: #force_inline proc() -> u32 {
	if _tls_active_ctx != nil && _tls_active_ctx.current_actor != nil {
		return intrinsics.atomic_load_explicit(&_tls_active_ctx.current_actor.generation, .Acquire)
	}
	return 0
}

// Returns the active `^Scheduler` bound to the current thread (via `init(&sched)`,
// `scoped_scheduler`, or the running worker thread).
current_scheduler :: #force_inline proc() -> ^Scheduler {
	if _tls_active_ctx != nil {
		return _tls_active_ctx.sched
	}
	return _tls_active_sched
}

// Explicitly bind `s` as the ambient scheduler on the calling thread.
use_scheduler :: #force_inline proc(s: ^Scheduler) {
	_tls_active_sched = s
}

// Intrusive base embedded at offset 0 of every concrete `Actor($State, $Msg)`.
// Allows a single `Scheduler` to queue and step heterogeneous actor types.
Actor_Base :: struct #align(64) {
	next:         ^Actor_Base, // Offset 0: Intrusive link for `RunQ_Shard(Actor_Base)`
	step_proc:    proc(base: ^Actor_Base, ctx: ^Context, quota: int) -> (processed: int, has_more: bool),
	destroy_proc: proc(base: ^Actor_Base),
	reg_sched:    ^Scheduler,
	user_ptr:     rawptr, // Optional user / supervisor context pointer
	scheduled:    u32,    // Atomic flag: 0 = idle, 1 = queued/running on scheduler
	id:           u32,    // Optional actor ID for routing / debugging
	status:       u32,    // Lifecycle status (used by Tracks 2–4 supervision)
	in_step:      u32,    // Non-zero while a worker thread is actively inside `step_proc`
	generation:   u32,    // Restart generation counter
	trap_panics:  b32,    // True when running under a supervisor crash boundary
}

#assert(size_of(Actor_Base) == 64)

// Execution context passed into an actor's `receive` callback (and bound to `_tls_active_ctx`).
Context :: struct {
	sched:              ^Scheduler,
	worker_id:          int,
	current_actor:      ^Actor_Base, // Currently executing actor base
	current_pools_raw:  rawptr,      // Backing slice pointer of current actor's `pools`
	current_msg_type:   typeid,      // Message payload type of the active turn
	current_env:        rawptr,      // Pointer to the active `^Envelope(Msg)` being processed
	current_reply_slot: rawptr,      // Optional `^Call_Slot(Res)` or `^Actor_Ref(Res)` for `actor.reply`
	current_flags:      u16,         // Active envelope's `flags` (`ENVELOPE_FLAG_*`)
	forwarded:          bool,        // Set to `true` when `forward` transfers ownership of `current_env`
}

// Stack-allocated reply slot used by synchronous `actor.call`.
// `ready` MUST remain at byte offset 0 for type-erased wake/cancellation during teardown.
Call_Slot :: struct($Res: typeid) {
	ready:    sync.Futex, // Offset 0: `CALL_STATE_*` handshake word
	res_type: typeid,
	result:   Res,
}

// Message-typed actor endpoint (type-erased over private `$State`).
// Embedded at offset 0 of `Actor($State, $Msg)` so any actor accepting `$Msg` can be
// addressed via `PID($Msg)` (`^Actor_Ref($Msg)`) without exposing its private `$State`.
Actor_Ref :: struct($Msg: typeid) #align(64) {
	using base: Actor_Base,
	mailbox:    MPSC_Queue(Envelope(Msg)),
	pools:      []Envelope_Pool(Msg),
	sched:      ^Scheduler,
	allocator:  mem.Allocator,
}

// Type-safe process identifier (address) for any actor accepting `$Msg`,
// decoupled from the actor's private internal `$State` type.
PID :: struct($Msg: typeid) {
	ref:        ^Actor_Ref(Msg),
	generation: u32,
}

// Obtain a message-typed `PID($Msg)` handle from any `^Actor($State, $Msg)`.
pid :: #force_inline proc(a: ^Actor($State, $Msg)) -> PID(Msg) {
	if a == nil {
		return {}
	}
	return PID(Msg){
		ref        = &a.ref,
		generation = intrinsics.atomic_load_explicit(&a.base.generation, .Acquire),
	}
}

// Concrete typed Actor parameterized over its private `$State` and `$Msg` type.
Actor :: struct($State: typeid, $Msg: typeid) #align(64) {
	using ref:      Actor_Ref(Msg),
	state:          State,
	receive:        proc(ctx: ^Context, self: ^Actor(State, Msg), msg: ^Msg),
	receive_state:  proc(ctx: ^Context, state: ^State, msg: ^Msg),
	receive_simple: proc(state: ^State, msg: ^Msg),
	receive_val:    proc(state: ^State, msg: Msg),
}

// Configuration options for `scheduler_init`.
Scheduler_Config :: struct {
	num_workers:    int,  // Defaults to `max(1, os.get_processor_core_count())` if <= 0
	num_shards:     int,  // Defaults to `max(num_workers, 1)` if <= 0
	batch_quota:    int,  // Messages processed per actor turn (default 64)
	enable_parking: bool, // True = sleep idle workers via `sync.Futex`; False = pure spin
}

@(private)
Worker_Thread_Ctx :: struct {
	sched:     ^Scheduler,
	worker_id: int,
}

@(private)
Auto_Pool_Entry :: struct {
	msg_type:     typeid,
	raw_ptr:      rawptr,
	count:        int,
	destroy_proc: proc(raw_ptr: rawptr, count: int, allocator: mem.Allocator),
}

// M:N work-stealing actor scheduler.
Scheduler :: struct #align(64) {
	shards:            []RunQ_Shard(Actor_Base),
	workers:           []^thread.Thread,
	worker_ctxs:       []Worker_Thread_Ctx,
	num_workers:       int,
	num_shards:        int,
	batch_quota:       int,
	enable_parking:    bool,
	started:           bool,
	allocator:         mem.Allocator,
	registry_mu:       sync.Mutex,
	auto_pools:        [dynamic]Auto_Pool_Entry,
	owned_actors:      [dynamic]^Actor_Base,
	prev_tls_sched:    ^Scheduler,

	// Global coordination & idle detection (cache-line isolated)
	_pad0:             [64]u8,
	active_actors:     i64,        // Number of currently scheduled actors across all shards
	pending_tokens:    i64,        // Optional user token/task countdown for `scheduler_wait_tokens`
	total_processed:   i64,        // Optional message counter for `scheduler_wait_messages`
	target_messages:   i64,        // Target message count (0 = disabled)
	running:           b32,        // False when shutting down
	parked_workers:    u32,        // Count of workers sleeping on `wake_futex`
	ext_shard_counter: u32,        // Round-robin shard counter for external sends
	wake_futex:        sync.Futex, // Futex signaled when an actor becomes runnable
}

// -----------------------------------------------------------------------------
// Scheduler Lifecycle & Scoped RAII Helpers
// -----------------------------------------------------------------------------

// Initialize a `Scheduler` with the given `Scheduler_Config` and `allocator`.
scheduler_init :: proc(
	s: ^Scheduler,
	cfg: Scheduler_Config = {},
	allocator := context.allocator,
) -> mem.Allocator_Error {
	s.allocator = allocator
	s.num_workers = cfg.num_workers if cfg.num_workers > 0 else max(1, os.get_processor_core_count())
	s.num_shards  = cfg.num_shards  if cfg.num_shards > 0  else s.num_workers
	s.batch_quota = cfg.batch_quota if cfg.batch_quota > 0 else DEFAULT_BATCH_QUOTA
	s.enable_parking = cfg.enable_parking
	s.started = false

	err: mem.Allocator_Error
	s.shards, err = make([]RunQ_Shard(Actor_Base), s.num_shards, allocator)
	if err != .None { return err }
	for i := 0; i < s.num_shards; i += 1 {
		shard_init(&s.shards[i])
	}

	s.workers, err = make([]^thread.Thread, s.num_workers, allocator)
	if err != .None {
		delete(s.shards, allocator)
		s.shards = nil
		return err
	}
	s.worker_ctxs, err = make([]Worker_Thread_Ctx, s.num_workers, allocator)
	if err != .None {
		delete(s.workers, allocator)
		s.workers = nil
		delete(s.shards, allocator)
		s.shards = nil
		return err
	}
	s.auto_pools, err = make([dynamic]Auto_Pool_Entry, 0, 8, allocator)
	if err != .None {
		delete(s.worker_ctxs, allocator)
		s.worker_ctxs = nil
		delete(s.workers, allocator)
		s.workers = nil
		delete(s.shards, allocator)
		s.shards = nil
		return err
	}
	s.owned_actors, err = make([dynamic]^Actor_Base, 0, 16, allocator)
	if err != .None {
		delete(s.auto_pools)
		s.auto_pools = nil
		delete(s.worker_ctxs, allocator)
		s.worker_ctxs = nil
		delete(s.workers, allocator)
		s.workers = nil
		delete(s.shards, allocator)
		s.shards = nil
		return err
	}

	s.running           = true
	s.active_actors     = 0
	s.pending_tokens    = 0
	s.total_processed   = 0
	s.target_messages   = 0
	s.parked_workers    = 0
	s.ext_shard_counter = 0
	s.wake_futex        = 0

	s.prev_tls_sched  = _tls_active_sched
	_tls_active_sched = s
	intrinsics.atomic_add_explicit(&_active_scheduler_count, 1, .Acq_Rel)
	return .None
}

// Convenience overload to initialize a `Scheduler` by specifying `num_workers` directly.
scheduler_init_workers :: proc(
	s: ^Scheduler,
	num_workers: int,
	enable_parking: bool = true,
	batch_quota: int = DEFAULT_BATCH_QUOTA,
	allocator := context.allocator,
) -> mem.Allocator_Error {
	return scheduler_init(s, Scheduler_Config{
		num_workers    = num_workers,
		num_shards     = num_workers,
		batch_quota    = batch_quota,
		enable_parking = enable_parking,
	}, allocator)
}

@(private)
_create_and_start_scheduler :: proc(
	cfg: Scheduler_Config = {},
	allocator := context.allocator,
) -> ^Scheduler {
	s := new(Scheduler, allocator)
	scheduler_init(s, cfg, allocator)
	scheduler_start(s)
	return s
}

// Single-line RAII scheduler scope (`@(deferred_out = scheduler_scope_end)`).
// Allocates, initializes, and starts a `Scheduler`, and automatically waits for
// idle, destroys all spawned actors and auto-pools, and stops worker threads at scope exit.
@(deferred_out = scheduler_scope_end)
scoped_scheduler_cfg :: proc(
	cfg: Scheduler_Config = {},
	allocator := context.allocator,
) -> ^Scheduler {
	return _create_and_start_scheduler(cfg, allocator)
}

@(deferred_out = scheduler_scope_end)
scoped_scheduler_workers :: proc(
	num_workers: int,
	enable_parking: bool = true,
	allocator := context.allocator,
) -> ^Scheduler {
	return _create_and_start_scheduler(Scheduler_Config{
		num_workers    = num_workers,
		num_shards     = num_workers,
		enable_parking = enable_parking,
	}, allocator)
}

scoped_scheduler :: proc {
	scoped_scheduler_cfg,
	scoped_scheduler_workers,
}

// Scope-exit cleanup callback for `scoped_scheduler`.
scheduler_scope_end :: proc(s: ^Scheduler) {
	if s != nil {
		scheduler_wait_idle(s)
		alloc := s.allocator
		scheduler_stop_and_destroy(s)
		free(s, alloc)
	}
}

// Spawn and start the scheduler's OS worker threads.
scheduler_start :: proc(s: ^Scheduler = nil) {
	sched := s if s != nil else current_scheduler()
	if sched == nil || sched.started { return }
	sched.started = true
	for w := 0; w < sched.num_workers; w += 1 {
		sched.worker_ctxs[w] = Worker_Thread_Ctx{
			sched     = sched,
			worker_id = w,
		}
		sched.workers[w] = thread.create(scheduler_worker_proc)
		sched.workers[w].data = &sched.worker_ctxs[w]
		// Inherit caller context (`logger`, `allocator`, custom `temp_allocator` in tests).
		// When using `default_temp_allocator_proc`, `core:thread`'s `_select_context_for_thread`
		// rebinds `temp_allocator.data` to each worker thread's own `@(thread_local)` instance.
		sched.workers[w].init_context = context
		thread.start(sched.workers[w])
	}
}

start :: scheduler_start

// Stop all worker threads, join them, destroy any auto-managed actors and pools,
// and free all scheduler memory.
scheduler_stop_and_destroy :: proc(s: ^Scheduler) {
	if s == nil { return }
	intrinsics.atomic_store_explicit(&s.running, false, .Seq_Cst)
	if s.enable_parking {
		intrinsics.atomic_add_explicit((^u32)(&s.wake_futex), 1, .Release)
		sync.futex_broadcast(&s.wake_futex)
	}
	if s.started {
		for w := 0; w < s.num_workers; w += 1 {
			if s.workers[w] != nil {
				thread.join(s.workers[w])
				thread.destroy(s.workers[w])
				s.workers[w] = nil
			}
		}
		s.started = false
	}
	if s.owned_actors != nil {
		for a in s.owned_actors {
			if a != nil {
				a.reg_sched = nil
				if a.destroy_proc != nil {
					a.destroy_proc(a)
				}
			}
		}
		delete(s.owned_actors)
		s.owned_actors = nil
	}
	if s.auto_pools != nil {
		for entry in s.auto_pools {
			if entry.destroy_proc != nil {
				entry.destroy_proc(entry.raw_ptr, entry.count, s.allocator)
			}
		}
		delete(s.auto_pools)
		s.auto_pools = nil
	}
	if s.workers != nil {
		delete(s.workers, s.allocator)
		s.workers = nil
	}
	if s.worker_ctxs != nil {
		delete(s.worker_ctxs, s.allocator)
		s.worker_ctxs = nil
	}
	if s.shards != nil {
		delete(s.shards, s.allocator)
		s.shards = nil
	}
	if _tls_active_sched == s {
		if s.prev_tls_sched != nil && intrinsics.atomic_load_explicit(&s.prev_tls_sched.running, .Acquire) {
			_tls_active_sched = s.prev_tls_sched
		} else {
			_tls_active_sched = nil
		}
	}
	_ = intrinsics.atomic_sub_explicit(&_active_scheduler_count, 1, .Acq_Rel)
	sync.rw_mutex_lock(&default_registry.mu)
	if intrinsics.atomic_load_explicit(&_active_scheduler_count, .Acquire) == 0 {
		if default_registry.entries != nil {
			for _, entry in default_registry.entries {
				detach_registry_hook_from_actor(entry.ref, rawptr(&default_registry))
			}
			delete(default_registry.entries)
			default_registry.entries = nil
		}
		default_registry.allocator = {}
	}
	sync.rw_mutex_unlock(&default_registry.mu)
}

// Look up or lazily create a shared `[]Envelope_Pool($Msg)` slice on `s` for `$Msg`.
// Layout: `pools[0 ..< num_workers]` are lock-free worker-local pools;
// `pools[len(pools) - 1]` is the dedicated `.busy`-guarded external pool.
@(private)
scheduler_get_or_create_pools :: proc(s: ^Scheduler, $Msg: typeid) -> []Envelope_Pool(Msg) {
	if s == nil { return nil }
	sync.mutex_lock(&s.registry_mu)
	defer sync.mutex_unlock(&s.registry_mu)

	tid := typeid_of(Msg)
	for entry in s.auto_pools {
		if entry.msg_type == tid {
			return mem.slice_ptr(cast(^Envelope_Pool(Msg))entry.raw_ptr, entry.count)
		}
	}

	num_pools := min(max(s.num_workers, 1) + 1, MAX_POOLS)
	pools, err := make([]Envelope_Pool(Msg), num_pools, s.allocator)
	if err != .None { return nil }
	for i := 0; i < num_pools; i += 1 {
		pool_init(&pools[i], u16(i), DEFAULT_AUTO_POOL_CAPACITY, s.allocator, heap_fallback = true)
	}

	append(&s.auto_pools, Auto_Pool_Entry{
		msg_type = tid,
		raw_ptr  = raw_data(pools),
		count    = len(pools),
		destroy_proc = proc(raw_ptr: rawptr, count: int, allocator: mem.Allocator) {
			slice := mem.slice_ptr(cast(^Envelope_Pool(Msg))raw_ptr, count)
			for i := 0; i < count; i += 1 {
				pool_destroy(&slice[i])
			}
			delete(slice, allocator)
		},
	})
	return pools
}

// Schedule an `Actor_Base` onto shard `shard_hint` if it is not already scheduled.
scheduler_schedule_actor :: #force_inline proc(s: ^Scheduler, a: ^Actor_Base, shard_hint: int = 0) {
	if s == nil || a == nil { return }
	if intrinsics.atomic_load_explicit(&a.status, .Seq_Cst) != u32(Actor_Status.Running) {
		return
	}
	if intrinsics.atomic_load_explicit(&a.scheduled, .Seq_Cst) == 0 {
		if intrinsics.atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0 {
			intrinsics.atomic_add_explicit(&s.active_actors, 1, .Acq_Rel)
			idx := u32(shard_hint) % u32(s.num_shards)
			shard_push(&s.shards[idx], a)

			if s.enable_parking {
				intrinsics.atomic_thread_fence(.Seq_Cst)
				if intrinsics.atomic_load_explicit(&s.parked_workers, .Acquire) > 0 {
					intrinsics.atomic_add_explicit((^u32)(&s.wake_futex), 1, .Release)
					sync.futex_signal(&s.wake_futex)
				}
			}
		}
	}
}

// Wait until all actors have drained their mailboxes and become idle (`active_actors == 0`).
scheduler_wait_idle :: proc(s: ^Scheduler = nil) {
	sched := s if s != nil else current_scheduler()
	if sched == nil { return }
	spin := 0
	for intrinsics.atomic_load_explicit(&sched.active_actors, .Acquire) > 0 {
		if sched.enable_parking && spin >= DEFAULT_FUTEX_SPIN_LIMIT {
			time.sleep(10 * time.Microsecond)
		} else {
			spin += 1
			intrinsics.cpu_relax()
		}
	}
}

wait_idle :: scheduler_wait_idle

// Wait until `pending_tokens` drops to 0 (useful for autonomous Actor-to-Actor graphs).
scheduler_wait_tokens :: proc(s: ^Scheduler = nil) {
	sched := s if s != nil else current_scheduler()
	if sched == nil { return }
	spin := 0
	for intrinsics.atomic_load_explicit(&sched.pending_tokens, .Acquire) > 0 {
		if sched.enable_parking && spin >= DEFAULT_FUTEX_SPIN_LIMIT {
			time.sleep(10 * time.Microsecond)
		} else {
			spin += 1
			intrinsics.cpu_relax()
		}
	}
}

wait_tokens :: scheduler_wait_tokens

// Wait until `total_processed` reaches `target_msgs`.
scheduler_wait_messages :: proc(s: ^Scheduler, target_msgs: i64) {
	spin := 0
	for intrinsics.atomic_load_explicit(&s.total_processed, .Acquire) < target_msgs {
		if s.enable_parking && spin >= DEFAULT_FUTEX_SPIN_LIMIT {
			time.sleep(10 * time.Microsecond)
		} else {
			spin += 1
			intrinsics.cpu_relax()
		}
	}
}

scheduler_wait_messages_ambient :: proc(target_msgs: i64) {
	s := current_scheduler()
	assert(s != nil, "No active Scheduler on current thread")
	scheduler_wait_messages(s, target_msgs)
}

wait_messages :: proc {
	scheduler_wait_messages,
	scheduler_wait_messages_ambient,
}

// Decrement the scheduler's active token counter (e.g. when a circulating message finishes its hops).
complete_token_ctx :: #force_inline proc(ctx: ^Context, count: i64 = 1) {
	if intrinsics.atomic_sub_explicit(&ctx.sched.pending_tokens, count, .Acq_Rel) == count {
		if ctx.sched.enable_parking {
			intrinsics.atomic_add_explicit((^u32)(&ctx.sched.wake_futex), 1, .Release)
			sync.futex_broadcast(&ctx.sched.wake_futex)
		}
	}
}

complete_token_ambient :: #force_inline proc(count: i64 = 1) {
	ctx := current_context()
	if ctx != nil {
		complete_token_ctx(ctx, count)
		return
	}
	s := current_scheduler()
	if s != nil {
		if intrinsics.atomic_sub_explicit(&s.pending_tokens, count, .Acq_Rel) == count {
			if s.enable_parking {
				intrinsics.atomic_add_explicit((^u32)(&s.wake_futex), 1, .Release)
				sync.futex_broadcast(&s.wake_futex)
			}
		}
	}
}

complete_token :: proc {
	complete_token_ctx,
	complete_token_ambient,
}

// -----------------------------------------------------------------------------
// Worker Loop (Sharded Run-Queue Pop + Work Stealing + Lock-Free Reschedule)
// -----------------------------------------------------------------------------

@(private)
scheduler_all_shards_empty :: #force_inline proc(s: ^Scheduler) -> bool {
	for i := 0; i < s.num_shards; i += 1 {
		if !mpsc_is_empty(&s.shards[i].q) {
			return false
		}
	}
	return true
}

@(private)
scheduler_worker_proc :: proc(t: ^thread.Thread) {
	wctx := cast(^Worker_Thread_Ctx)t.data
	s    := wctx.sched
	wid  := wctx.worker_id
	ctx  := Context{
		sched     = s,
		worker_id = wid,
	}
	context.allocator = s.allocator
	_tls_active_ctx   = &ctx
	_tls_active_sched = s
	defer {
		free_all(context.temp_allocator)
		if context.temp_allocator.procedure == runtime.default_temp_allocator_proc && context.temp_allocator.data != nil {
			runtime.default_temp_allocator_destroy(cast(^runtime.Default_Temp_Allocator)context.temp_allocator.data)
		}
		_tls_active_ctx   = nil
		_tls_active_sched = nil
	}

	spin_count := 0
	num_shards := s.num_shards
	quota      := s.batch_quota

	for intrinsics.atomic_load_explicit(&s.running, .Acquire) {
		// 1. Pop from worker's home shard, then steal from peer shards
		a := shard_pop_or_steal(&s.shards[wid % num_shards])
		if a == nil && num_shards > 1 {
			for i := 1; i < num_shards; i += 1 {
				peer := (wid + i) % num_shards
				a = shard_pop_or_steal(&s.shards[peer])
				if a != nil { break }
			}
		}

		if a == nil {
			if !s.enable_parking {
				intrinsics.cpu_relax()
			} else {
				if spin_count < DEFAULT_FUTEX_SPIN_LIMIT {
					spin_count += 1
					intrinsics.cpu_relax()
				} else {
					val := intrinsics.atomic_load_explicit((^u32)(&s.wake_futex), .Acquire)
					intrinsics.atomic_add_explicit(&s.parked_workers, 1, .Seq_Cst)
					if intrinsics.atomic_load_explicit(&s.running, .Acquire) &&
					   (intrinsics.atomic_load_explicit(&s.active_actors, .Seq_Cst) == 0 || scheduler_all_shards_empty(s)) {
						sync.futex_wait(&s.wake_futex, val)
					}
					intrinsics.atomic_sub_explicit(&s.parked_workers, 1, .Seq_Cst)
					spin_count = 0
				}
			}
			continue
		}

		spin_count = 0

		// 2. Execute up to `quota` messages on the actor, then reclaim outer-turn temp memory.
		processed, has_more := a.step_proc(a, &ctx, quota)
		when !runtime.NO_DEFAULT_TEMP_ALLOCATOR {
			if context.temp_allocator.procedure != runtime.default_temp_allocator_proc {
				free_all(context.temp_allocator)
			} else if ta := cast(^runtime.Default_Temp_Allocator)context.temp_allocator.data; ta != nil && ta.arena.total_used > 0 {
				free_all(context.temp_allocator)
			}
		} else {
			free_all(context.temp_allocator)
		}

		// 3. Reschedule or unschedule with `.Seq_Cst` Dekker barrier before decrementing `active_actors`
		if has_more {
			shard_push(&s.shards[wid % num_shards], a)
			if s.enable_parking {
				intrinsics.atomic_thread_fence(.Seq_Cst)
				if intrinsics.atomic_load_explicit(&s.parked_workers, .Acquire) > 0 {
					intrinsics.atomic_add_explicit((^u32)(&s.wake_futex), 1, .Release)
					sync.futex_signal(&s.wake_futex)
				}
			}
		} else {
			intrinsics.atomic_store_explicit(&a.scheduled, 0, .Seq_Cst)
			_, still_has_more := a.step_proc(a, &ctx, 0)
			if still_has_more {
				if intrinsics.atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0 {
					shard_push(&s.shards[wid % num_shards], a)
					if s.enable_parking {
						intrinsics.atomic_thread_fence(.Seq_Cst)
						if intrinsics.atomic_load_explicit(&s.parked_workers, .Acquire) > 0 {
							intrinsics.atomic_add_explicit((^u32)(&s.wake_futex), 1, .Release)
							sync.futex_signal(&s.wake_futex)
						}
					}
				} else {
					intrinsics.atomic_sub_explicit(&s.active_actors, 1, .Seq_Cst)
				}
			} else {
				intrinsics.atomic_sub_explicit(&s.active_actors, 1, .Seq_Cst)
			}
		}

		if processed > 0 {
			intrinsics.atomic_add_explicit(&s.total_processed, i64(processed), .Release)
		}
	}
}

// -----------------------------------------------------------------------------
// Typed Actor Initialization, Step Procedure, Sending, and Zero-Copy Forwarding
// -----------------------------------------------------------------------------

@(private)
actor_init_common :: proc(
	a: ^Actor($State, $Msg),
	initial_state: State,
	step_mode: int,
	pools: []Envelope_Pool(Msg) = nil,
	id: u32 = 0,
	s: ^Scheduler = nil,
	allocator := context.allocator,
) {
	sched := s if s != nil else current_scheduler()
	eff_alloc := resolve_spawn_allocator(sched, allocator)
	a.base.next         = nil
	a.base.scheduled    = 0
	a.base.id           = id
	a.base.status       = 0
	a.base.in_step      = 0
	a.base.generation   = 1
	a.base.trap_panics  = false
	a.base.reg_sched    = nil
	a.base.destroy_proc = nil
	a.base.user_ptr     = nil
	switch step_mode {
	case STEP_MODE_STATE:
		a.base.step_proc = actor_generic_step_proc(State, Msg, STEP_MODE_STATE)
	case STEP_MODE_SIMPLE:
		a.base.step_proc = actor_generic_step_proc(State, Msg, STEP_MODE_SIMPLE)
	case STEP_MODE_VAL:
		a.base.step_proc = actor_generic_step_proc(State, Msg, STEP_MODE_VAL)
	case:
		a.base.step_proc = actor_generic_step_proc(State, Msg, STEP_MODE_ACTOR)
	}
	mpsc_init(&a.mailbox)
	a.state     = initial_state
	a.sched     = sched
	a.allocator = eff_alloc
	if pools != nil {
		a.pools = pools
	} else if sched != nil {
		a.pools = scheduler_get_or_create_pools(sched, Msg)
	}
}

// 1. Initialize with 3-arg `proc(ctx: ^Context, self: ^Actor(State, Msg), msg: ^Msg)`
actor_init :: proc(
	a: ^Actor($State, $Msg),
	initial_state: State,
	receive_proc: proc(ctx: ^Context, self: ^Actor(State, Msg), msg: ^Msg),
	pools: []Envelope_Pool(Msg) = nil,
	id: u32 = 0,
	allocator := context.allocator,
) {
	actor_init_common(a, initial_state, STEP_MODE_ACTOR, pools, id, nil, allocator)
	a.receive = receive_proc
}

// 2. Initialize with 3-arg `proc(ctx: ^Context, state: ^State, msg: ^Msg)`
actor_init_state :: proc(
	a: ^Actor($State, $Msg),
	initial_state: State,
	receive_proc: proc(ctx: ^Context, state: ^State, msg: ^Msg),
	pools: []Envelope_Pool(Msg) = nil,
	id: u32 = 0,
	allocator := context.allocator,
) {
	actor_init_common(a, initial_state, STEP_MODE_STATE, pools, id, nil, allocator)
	a.receive_state = receive_proc
}

// 3. Initialize with 2-arg `proc(state: ^State, msg: ^Msg)`
actor_init_simple :: proc(
	a: ^Actor($State, $Msg),
	initial_state: State,
	receive_proc: proc(state: ^State, msg: ^Msg),
	pools: []Envelope_Pool(Msg) = nil,
	id: u32 = 0,
	allocator := context.allocator,
) {
	actor_init_common(a, initial_state, STEP_MODE_SIMPLE, pools, id, nil, allocator)
	a.receive_simple = receive_proc
}

// 4. Initialize with 2-arg value `proc(state: ^State, msg: Msg)` (ideal for `union` behaviors)
actor_init_val :: proc(
	a: ^Actor($State, $Msg),
	initial_state: State,
	receive_proc: proc(state: ^State, msg: Msg),
	pools: []Envelope_Pool(Msg) = nil,
	id: u32 = 0,
	allocator := context.allocator,
) where !intrinsics.type_is_pointer(Msg) {
	actor_init_common(a, initial_state, STEP_MODE_VAL, pools, id, nil, allocator)
	a.receive_val = receive_proc
}

@(private)
register_spawned_actor :: proc(a: ^Actor($State, $Msg), s: ^Scheduler) {
	if s == nil { return }
	a.base.reg_sched = s
	a.base.destroy_proc = proc(base: ^Actor_Base) {
		self := cast(^Actor(State, Msg))base
		actor_cleanup_supervision(&self.base)
		actor_drain_mailbox(self)
		free(self, self.allocator)
	}
	sync.mutex_lock(&s.registry_mu)
	append(&s.owned_actors, &a.base)
	sync.mutex_unlock(&s.registry_mu)
}

// Spawn Overload 1a: 3-arg `^Actor` callback (auto-pooled, ambient scheduler)
@(require_results)
spawn_actor_auto :: proc(
	initial_state: $State,
	receive_proc: proc(ctx: ^Context, self: ^Actor(State, $Msg), msg: ^Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> (a: ^Actor(State, Msg), err: mem.Allocator_Error) #optional_allocator_error {
	s := current_scheduler()
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a = new(Actor(State, Msg), eff_alloc) or_return
	actor_init_common(a, initial_state, STEP_MODE_ACTOR, nil, id, s, eff_alloc)
	a.receive = receive_proc
	register_spawned_actor(a, s)
	return a, .None
}

// Spawn Overload 1b: 3-arg `^Actor` callback (explicit `pools`, ambient scheduler)
@(require_results)
spawn_actor_pooled :: proc(
	initial_state: $State,
	receive_proc: proc(ctx: ^Context, self: ^Actor(State, $Msg), msg: ^Msg),
	pools: []Envelope_Pool(Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> (a: ^Actor(State, Msg), err: mem.Allocator_Error) #optional_allocator_error {
	s := current_scheduler()
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a = new(Actor(State, Msg), eff_alloc) or_return
	actor_init_common(a, initial_state, STEP_MODE_ACTOR, pools, id, s, eff_alloc)
	a.receive = receive_proc
	register_spawned_actor(a, s)
	return a, .None
}

// Spawn Overload 1c: 3-arg `^Actor` callback (explicit scheduler, auto-pooled)
@(require_results)
spawn_sched_actor_auto :: proc(
	s: ^Scheduler,
	initial_state: $State,
	receive_proc: proc(ctx: ^Context, self: ^Actor(State, $Msg), msg: ^Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> (a: ^Actor(State, Msg), err: mem.Allocator_Error) #optional_allocator_error {
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a = new(Actor(State, Msg), eff_alloc) or_return
	actor_init_common(a, initial_state, STEP_MODE_ACTOR, nil, id, s, eff_alloc)
	a.receive = receive_proc
	register_spawned_actor(a, s)
	return a, .None
}

// Spawn Overload 1d: 3-arg `^Actor` callback (explicit scheduler, explicit `pools`)
@(require_results)
spawn_sched_actor_pooled :: proc(
	s: ^Scheduler,
	initial_state: $State,
	receive_proc: proc(ctx: ^Context, self: ^Actor(State, $Msg), msg: ^Msg),
	pools: []Envelope_Pool(Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> (a: ^Actor(State, Msg), err: mem.Allocator_Error) #optional_allocator_error {
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a = new(Actor(State, Msg), eff_alloc) or_return
	actor_init_common(a, initial_state, STEP_MODE_ACTOR, pools, id, s, eff_alloc)
	a.receive = receive_proc
	register_spawned_actor(a, s)
	return a, .None
}

// Spawn Overload 2a: 3-arg `^State` callback (auto-pooled, ambient scheduler)
@(require_results)
spawn_state_auto :: proc(
	initial_state: $State,
	receive_proc: proc(ctx: ^Context, state: ^State, msg: ^$Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> (a: ^Actor(State, Msg), err: mem.Allocator_Error) #optional_allocator_error {
	s := current_scheduler()
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a = new(Actor(State, Msg), eff_alloc) or_return
	actor_init_common(a, initial_state, STEP_MODE_STATE, nil, id, s, eff_alloc)
	a.receive_state = receive_proc
	register_spawned_actor(a, s)
	return a, .None
}

// Spawn Overload 2b: 3-arg `^State` callback (explicit scheduler, auto-pooled)
@(require_results)
spawn_sched_state_auto :: proc(
	s: ^Scheduler,
	initial_state: $State,
	receive_proc: proc(ctx: ^Context, state: ^State, msg: ^$Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> (a: ^Actor(State, Msg), err: mem.Allocator_Error) #optional_allocator_error {
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a = new(Actor(State, Msg), eff_alloc) or_return
	actor_init_common(a, initial_state, STEP_MODE_STATE, nil, id, s, eff_alloc)
	a.receive_state = receive_proc
	register_spawned_actor(a, s)
	return a, .None
}

// Spawn Overload 3a: 2-arg `(state: ^State, msg: ^Msg)` callback (auto-pooled, ambient scheduler)
@(require_results)
spawn_simple_auto :: proc(
	initial_state: $State,
	receive_proc: proc(state: ^State, msg: ^$Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> (a: ^Actor(State, Msg), err: mem.Allocator_Error) #optional_allocator_error {
	s := current_scheduler()
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a = new(Actor(State, Msg), eff_alloc) or_return
	actor_init_common(a, initial_state, STEP_MODE_SIMPLE, nil, id, s, eff_alloc)
	a.receive_simple = receive_proc
	register_spawned_actor(a, s)
	return a, .None
}

// Spawn Overload 3b: 2-arg `(state: ^State, msg: ^Msg)` callback (explicit scheduler, auto-pooled)
@(require_results)
spawn_sched_simple_auto :: proc(
	s: ^Scheduler,
	initial_state: $State,
	receive_proc: proc(state: ^State, msg: ^$Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> (a: ^Actor(State, Msg), err: mem.Allocator_Error) #optional_allocator_error {
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a = new(Actor(State, Msg), eff_alloc) or_return
	actor_init_common(a, initial_state, STEP_MODE_SIMPLE, nil, id, s, eff_alloc)
	a.receive_simple = receive_proc
	register_spawned_actor(a, s)
	return a, .None
}

// Spawn Overload 4a: 2-arg value `(state: ^State, msg: Msg)` callback (auto-pooled, ambient scheduler)
@(require_results)
spawn_val_auto :: proc(
	initial_state: $State,
	receive_proc: proc(state: ^State, msg: $Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> ^Actor(State, Msg) where !intrinsics.type_is_pointer(Msg) {
	s := current_scheduler()
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a := new(Actor(State, Msg), eff_alloc)
	if a == nil {
		return nil
	}
	actor_init_common(a, initial_state, STEP_MODE_VAL, nil, id, s, eff_alloc)
	a.receive_val = receive_proc
	register_spawned_actor(a, s)
	return a
}

// Spawn Overload 4b: 2-arg value `(state: ^State, msg: Msg)` callback (explicit scheduler, auto-pooled)
@(require_results)
spawn_sched_val_auto :: proc(
	s: ^Scheduler,
	initial_state: $State,
	receive_proc: proc(state: ^State, msg: $Msg),
	id: u32 = 0,
	allocator := context.allocator,
) -> ^Actor(State, Msg) where !intrinsics.type_is_pointer(Msg) {
	eff_alloc := resolve_spawn_allocator(s, allocator)
	a := new(Actor(State, Msg), eff_alloc)
	if a == nil {
		return nil
	}
	actor_init_common(a, initial_state, STEP_MODE_VAL, nil, id, s, eff_alloc)
	a.receive_val = receive_proc
	register_spawned_actor(a, s)
	return a
}

spawn :: proc {
	spawn_actor_auto,
	spawn_actor_pooled,
	spawn_sched_actor_auto,
	spawn_sched_actor_pooled,
	spawn_state_auto,
	spawn_sched_state_auto,
	spawn_simple_auto,
	spawn_sched_simple_auto,
	spawn_val_auto,
	spawn_sched_val_auto,
}

// Compile-time structural typing (`base:intrinsics`): spawn an actor directly from a
// state struct `State` that defines a `receive: proc(self: ^State, msg: Msg)` field.
@(require_results)
spawn_self :: proc(
	initial_state: $State,
	id: u32 = 0,
	allocator := context.allocator,
) -> ^Actor(State, intrinsics.type_proc_parameter_type(intrinsics.type_field_type(State, "receive"), 1))
	where intrinsics.type_has_field(State, "receive") {
	return spawn_val_auto(initial_state, initial_state.receive, id, allocator)
}

// Retire a consumed or drained envelope (wake pending `actor.call` waiters as faulted if
// un-replied, then free heap nodes or append pooled nodes to `collector`).
@(private)
step_retire_envelope :: #force_inline proc(
	ref: ^Actor_Ref($Msg),
	env: ^Envelope(Msg),
	flags: u16,
	reply_slot: rawptr,
	collector: ^Batch_Return_Collector(Msg),
	has_pools: bool,
) {
	if (flags & ENVELOPE_FLAG_CALL) != 0 && reply_slot != nil {
		complete_call_slot(reply_slot, CALL_STATE_FAULTED)
	}
	if (env.flags & ENVELOPE_FLAG_HEAP) != 0 {
		pid := env.pool_id
		alloc := ref.pools[pid].allocator if (has_pools && int(pid) < len(ref.pools)) else ref.allocator
		if alloc.procedure == nil {
			alloc = context.allocator
		}
		free(env, alloc)
	} else if has_pools && (env.flags & ENVELOPE_FLAG_STATIC) == 0 && int(env.pool_id) < len(ref.pools) {
		collector_add(collector, env)
	}
}

// Pop and reclaim all remaining envelopes in an actor's mailbox, waking any
// queued synchronous `actor.call` waiters with `CALL_STATE_FAULTED`.
@(private)
actor_ref_drain_mailbox :: proc(ref: ^Actor_Ref($Msg)) {
	if ref == nil { return }
	collector: Batch_Return_Collector(Msg) = ---
	collector.used_mask = 0
	has_pools := len(ref.pools) > 0
	for {
		env := mpsc_pop_detached(&ref.mailbox)
		if env == nil {
			if !mpsc_is_empty(&ref.mailbox) {
				intrinsics.cpu_relax()
				continue
			}
			break
		}
		step_retire_envelope(ref, env, env.flags, env.reply_slot, &collector, has_pools)
	}
	if has_pools {
		collector_flush(&collector, ref.pools, -1)
	}
}

@(private)
actor_drain_mailbox :: proc(a: ^Actor($State, $Msg)) {
	if a == nil { return }
	actor_ref_drain_mailbox(&a.ref)
}

// Free a heap-allocated `Actor($State, $Msg)` created via `spawn`.
// Safe to call explicitly via `defer actor.destroy(a)` OR omit entirely when using
// a `Scheduler` (which automatically destroys all remaining registered actors on shutdown).
actor_destroy :: proc(a: ^Actor($State, $Msg)) {
	if a == nil { return }
	if a.base.destroy_proc != nil {
		actor_base_destroy(&a.base)
		return
	}
	s := a.base.reg_sched
	if s != nil {
		if s.started && intrinsics.atomic_load_explicit(&s.running, .Acquire) {
			for intrinsics.atomic_load_explicit(&a.base.scheduled, .Acquire) != 0 ||
			    intrinsics.atomic_load_explicit(&a.base.in_step, .Seq_Cst) != 0 {
				intrinsics.cpu_relax()
			}
		}
		sync.mutex_lock(&s.registry_mu)
		for i := 0; i < len(s.owned_actors); i += 1 {
			if s.owned_actors[i] == &a.base {
				unordered_remove(&s.owned_actors, i)
				break
			}
		}
		sync.mutex_unlock(&s.registry_mu)
		a.base.reg_sched = nil
	}
	actor_cleanup_supervision(&a.base)
	actor_drain_mailbox(a)
	free(a, a.allocator)
}

@(private)
actor_step_trapped :: #force_no_inline proc(
	self: ^Actor($State, $Msg),
	base: ^Actor_Base,
	ctx: ^Context,
	quota: int,
	collector: ^Batch_Return_Collector(Msg),
	has_pools: bool,
	$Mode: int,
) -> (int, bool) {
	processed := 0
	for processed < quota {
		if intrinsics.atomic_load_explicit(&base.status, .Seq_Cst) != u32(Actor_Status.Running) {
			break
		}
		env := mpsc_pop_detached(&self.mailbox)
		if env == nil { break }

		ctx.current_env        = env
		ctx.current_reply_slot = env.reply_slot
		ctx.current_flags      = env.flags
		ctx.forwarded          = false

		trapped, normal_exit, fault := execute_trapped_actor_turn(self, env, ctx, Mode)
		processed += 1
		if trapped {
			reply_slot := ctx.current_reply_slot
			flags := ctx.current_flags
			if !ctx.forwarded {
				step_retire_envelope(&self.ref, env, 0, nil, collector, has_pools)
			}
			if has_pools {
				collector_flush(collector, self.pools, -1)
			}

			ctx.current_env        = nil
			ctx.current_reply_slot = nil
			ctx.current_flags      = 0
			ctx.current_pools_raw  = nil
			ctx.current_msg_type   = nil

			// Keep `ctx.current_actor = base` across `handle_actor_turn_exit` so
			// `finalize_actor_stopped` sees `is_self_step == true` while `in_step == 1`.
			handle_actor_turn_exit(base, normal_exit, fault)
			if (flags & ENVELOPE_FLAG_CALL) != 0 && reply_slot != nil {
				complete_call_slot(reply_slot, CALL_STATE_FAULTED)
			}
			return processed, false
		}

		if !ctx.forwarded {
			step_retire_envelope(&self.ref, env, ctx.current_flags, ctx.current_reply_slot, collector, has_pools)
		}
	}

	if has_pools {
		collector_flush(collector, self.pools, -1)
	}
	if intrinsics.atomic_load_explicit(&base.status, .Seq_Cst) != u32(Actor_Status.Running) {
		return processed, false
	}
	return processed, !mpsc_is_empty(&self.mailbox)
}

// Generates the specialized type-erased step procedure for `Actor($State, $Msg)`.
// When `quota == 0`, acts as a pure non-consuming readiness probe (`!mpsc_is_empty(&a.mailbox)`).
@(private)
actor_generic_step_proc :: proc(
	$State: typeid,
	$Msg: typeid,
	$Mode: int,
) -> proc(base: ^Actor_Base, ctx: ^Context, quota: int) -> (int, bool) {
	return proc(base: ^Actor_Base, ctx: ^Context, quota: int) -> (int, bool) {
		self := cast(^Actor(State, Msg))base
		if quota == 0 {
			if intrinsics.atomic_load_explicit(&base.status, .Seq_Cst) != u32(Actor_Status.Running) {
				return 0, false
			}
			return 0, !mpsc_is_empty(&self.mailbox)
		}

		intrinsics.atomic_store_explicit(&base.in_step, 1, .Seq_Cst)
		defer intrinsics.atomic_store_explicit(&base.in_step, 0, .Seq_Cst)

		collector: Batch_Return_Collector(Msg) = ---
		collector.used_mask = 0
		has_pools := len(self.pools) > 0
		processed := 0

		ctx.current_actor     = base
		ctx.current_pools_raw = raw_data(self.pools)
		ctx.current_msg_type  = typeid_of(Msg)
		defer {
			ctx.current_env        = nil
			ctx.current_reply_slot = nil
			ctx.current_flags      = 0
			ctx.current_actor      = nil
			ctx.current_pools_raw  = nil
			ctx.current_msg_type   = nil
			ctx.forwarded          = false
		}

		if base.trap_panics || base.user_ptr != nil {
			return actor_step_trapped(self, base, ctx, quota, &collector, has_pools, Mode)
		}

		for processed < quota {
			if intrinsics.atomic_load_explicit(&base.status, .Seq_Cst) != u32(Actor_Status.Running) {
				break
			}
			if base.trap_panics || base.user_ptr != nil {
				p2, has_more := actor_step_trapped(self, base, ctx, quota - processed, &collector, has_pools, Mode)
				return processed + p2, has_more
			}
			env := mpsc_pop_detached(&self.mailbox)
			if env == nil { break }

			ctx.current_env        = env
			ctx.current_reply_slot = env.reply_slot
			ctx.current_flags      = env.flags
			ctx.forwarded          = false

			when Mode == STEP_MODE_ACTOR {
				self.receive(ctx, self, &env.msg)
			} else when Mode == STEP_MODE_STATE {
				self.receive_state(ctx, &self.state, &env.msg)
			} else when Mode == STEP_MODE_SIMPLE {
				self.receive_simple(&self.state, &env.msg)
			} else when Mode == STEP_MODE_VAL {
				self.receive_val(&self.state, env.msg)
			}
			processed += 1

			if !ctx.forwarded {
				step_retire_envelope(&self.ref, env, ctx.current_flags, ctx.current_reply_slot, &collector, has_pools)
			}
		}

		if processed > 0 && has_pools {
			collector_flush(&collector, self.pools, -1)
		}
		if intrinsics.atomic_load_explicit(&base.status, .Seq_Cst) != u32(Actor_Status.Running) {
			return processed, false
		}
		return processed, !mpsc_is_empty(&self.mailbox)
	}
}

// -----------------------------------------------------------------------------
// Auto-Pooled Envelope Acquisition Helpers
// -----------------------------------------------------------------------------

@(private)
resolve_target_scheduler :: #force_inline proc(s: ^Scheduler, target: ^Actor_Ref($Msg)) -> ^Scheduler {
	if target != nil && target.sched != nil { return target.sched }
	if s != nil { return s }
	return current_scheduler()
}

@(private)
acquire_auto_envelope :: #force_inline proc(
	s: ^Scheduler,
	target: ^Actor_Ref($Msg),
	shard_hint: int = -1,
) -> (env: ^Envelope(Msg), hint: int) {
	if len(target.pools) == 0 && s != nil {
		target.pools = scheduler_get_or_create_pools(s, Msg)
	}
	pools := target.pools
	ctx := _tls_active_ctx
	if ctx != nil && ctx.sched == s && ctx.worker_id >= 0 && ctx.worker_id + 1 < len(pools) {
		wid := ctx.worker_id
		e := pool_acquire(&pools[wid], spin_if_exhausted = false)
		return e, (wid if shard_hint < 0 else shard_hint)
	}
	eff_hint := shard_hint
	if eff_hint < 0 && s != nil {
		eff_hint = int(intrinsics.atomic_add_explicit(&s.ext_shard_counter, 1, .Relaxed))
	} else if eff_hint < 0 {
		eff_hint = 0
	}
	if len(pools) > 0 {
		ext_idx := len(pools) - 1
		ext_pool := &pools[ext_idx]
		if intrinsics.atomic_exchange_explicit(&ext_pool.busy, 1, .Acquire) == 0 {
			e := pool_acquire(ext_pool, spin_if_exhausted = false)
			intrinsics.atomic_store_explicit(&ext_pool.busy, 0, .Release)
			return e, eff_hint
		}
	}
	alloc := target.allocator if target.allocator.procedure != nil else context.allocator
	e := new(Envelope(Msg), alloc)
	e.next       = nil
	e.pool_id    = POOL_ID_NONE
	e.flags      = ENVELOPE_FLAG_HEAP
	e.reply_slot = nil
	return e, eff_hint
}

@(private)
acquire_auto_envelope_chain :: #force_inline proc(
	s: ^Scheduler,
	target: ^Actor_Ref($Msg),
	msgs: []Msg,
	shard_hint: int = -1,
) -> (first: ^Envelope(Msg), last: ^Envelope(Msg), hint: int) {
	if len(target.pools) == 0 && s != nil {
		target.pools = scheduler_get_or_create_pools(s, Msg)
	}
	pools := target.pools
	ctx := _tls_active_ctx
	if ctx != nil && ctx.sched == s && ctx.worker_id >= 0 && ctx.worker_id + 1 < len(pools) {
		wid := ctx.worker_id
		f, l := pool_acquire_chain(&pools[wid], msgs, spin_if_exhausted = false)
		return f, l, (wid if shard_hint < 0 else shard_hint)
	}
	eff_hint := shard_hint
	if eff_hint < 0 && s != nil {
		eff_hint = int(intrinsics.atomic_add_explicit(&s.ext_shard_counter, 1, .Relaxed))
	} else if eff_hint < 0 {
		eff_hint = 0
	}
	if len(pools) > 0 {
		ext_idx := len(pools) - 1
		ext_pool := &pools[ext_idx]
		if intrinsics.atomic_exchange_explicit(&ext_pool.busy, 1, .Acquire) == 0 {
			f, l := pool_acquire_chain(ext_pool, msgs, spin_if_exhausted = false)
			intrinsics.atomic_store_explicit(&ext_pool.busy, 0, .Release)
			return f, l, eff_hint
		}
	}
	alloc := target.allocator if target.allocator.procedure != nil else context.allocator
	first = new(Envelope(Msg), alloc)
	first.next       = nil
	first.pool_id    = POOL_ID_NONE
	first.flags      = ENVELOPE_FLAG_HEAP
	first.reply_slot = nil
	first.msg        = msgs[0]
	prev := first
	for i := 1; i < len(msgs); i += 1 {
		cur := new(Envelope(Msg), alloc)
		cur.next       = nil
		cur.pool_id    = POOL_ID_NONE
		cur.flags      = ENVELOPE_FLAG_HEAP
		cur.reply_slot = nil
		cur.msg        = msgs[i]
		intrinsics.atomic_store_explicit(&prev.next, cur, .Relaxed)
		prev = cur
	}
	return first, prev, eff_hint
}

// -----------------------------------------------------------------------------
// Message Sending Overloads (Explicit Pool, Auto-Pooled, `ctx`, Ambient, & `PID`)
// -----------------------------------------------------------------------------

@(private)
actor_base_can_accept :: #force_inline proc(base: ^Actor_Base, pid_gen: u32) -> bool {
	if base == nil {
		return false
	}
	if intrinsics.atomic_load_explicit(&base.status, .Acquire) == u32(Actor_Status.Stopped) {
		return false
	}
	if pid_gen != 0 && intrinsics.atomic_load_explicit(&base.generation, .Acquire) != pid_gen {
		return false
	}
	return true
}

@(private)
actor_recycle_single_envelope :: proc(ref: ^Actor_Ref($Msg), env: ^Envelope(Msg)) {
	if env == nil { return }
	if (env.flags & ENVELOPE_FLAG_CALL) != 0 && env.reply_slot != nil {
		complete_call_slot(env.reply_slot, CALL_STATE_FAULTED)
	}
	if (env.flags & ENVELOPE_FLAG_HEAP) != 0 {
		pid := env.pool_id
		alloc := ref.pools[pid].allocator if (ref != nil && len(ref.pools) > 0 && int(pid) < len(ref.pools)) else (ref.allocator if ref != nil else context.allocator)
		if alloc.procedure == nil {
			alloc = context.allocator
		}
		free(env, alloc)
	} else if ref != nil && len(ref.pools) > 0 && (env.flags & ENVELOPE_FLAG_STATIC) == 0 && int(env.pool_id) < len(ref.pools) {
		mpsc_push(&ref.pools[env.pool_id].remote_returns, env)
	}
}

@(private)
actor_recycle_envelope_chain :: proc(ref: ^Actor_Ref($Msg), first: ^Envelope(Msg)) {
	cur := first
	for cur != nil {
		nxt := intrinsics.atomic_load_explicit(&cur.next, .Relaxed)
		actor_recycle_single_envelope(ref, cur)
		cur = nxt
	}
}

// Send a message to `target` using a pre-allocated or pooled `^Envelope(Msg)`.
send_envelope :: #force_inline proc(
	s: ^Scheduler,
	target: ^Actor($State, $Msg),
	env: ^Envelope(Msg),
	shard_hint: int = 0,
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) {
		actor_recycle_single_envelope(&target.ref if target != nil else nil, env)
		return
	}
	mpsc_push(&target.mailbox, env)
	scheduler_schedule_actor(s, &target.base, shard_hint)
	if intrinsics.atomic_load_explicit(&target.base.status, .Seq_Cst) == u32(Actor_Status.Stopped) {
		actor_drain_stopped_target(&target.base, false)
	}
}

send_envelope_pid :: #force_inline proc(
	s: ^Scheduler,
	target: PID($Msg),
	env: ^Envelope(Msg),
	shard_hint: int = 0,
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) {
		actor_recycle_single_envelope(target.ref, env)
		return
	}
	mpsc_push(&target.ref.mailbox, env)
	scheduler_schedule_actor(s, &target.ref.base, shard_hint)
	if intrinsics.atomic_load_explicit(&target.ref.base.status, .Seq_Cst) == u32(Actor_Status.Stopped) {
		actor_drain_stopped_target(&target.ref.base, false)
	}
}

send_ambient_envelope :: #force_inline proc(
	target: ^Actor($State, $Msg),
	env: ^Envelope(Msg),
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) {
		actor_recycle_single_envelope(&target.ref if target != nil else nil, env)
		return
	}
	s := resolve_target_scheduler(nil, &target.ref)
	send_envelope(s, target, env, 0)
}

send_ambient_envelope_pid :: #force_inline proc(
	target: PID($Msg),
	env: ^Envelope(Msg),
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) {
		actor_recycle_single_envelope(target.ref, env)
		return
	}
	s := resolve_target_scheduler(nil, target.ref)
	send_envelope_pid(s, target, env, 0)
}

// Splice a pre-linked chain of envelopes `[first .. last]` onto `target.mailbox`
// in a single atomic `XCHG` (`pony_sendv`) and schedule `target`.
send_envelope_chain :: #force_inline proc(
	s: ^Scheduler,
	target: ^Actor($State, $Msg),
	first: ^Envelope(Msg),
	last: ^Envelope(Msg),
	shard_hint: int = 0,
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) {
		actor_recycle_envelope_chain(&target.ref if target != nil else nil, first)
		return
	}
	mpsc_push_chain(&target.mailbox, first, last)
	scheduler_schedule_actor(s, &target.base, shard_hint)
	if intrinsics.atomic_load_explicit(&target.base.status, .Seq_Cst) == u32(Actor_Status.Stopped) {
		actor_drain_stopped_target(&target.base, false)
	}
}

send_envelope_chain_pid :: #force_inline proc(
	s: ^Scheduler,
	target: PID($Msg),
	first: ^Envelope(Msg),
	last: ^Envelope(Msg),
	shard_hint: int = 0,
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) {
		actor_recycle_envelope_chain(target.ref, first)
		return
	}
	mpsc_push_chain(&target.ref.mailbox, first, last)
	scheduler_schedule_actor(s, &target.ref.base, shard_hint)
	if intrinsics.atomic_load_explicit(&target.ref.base.status, .Seq_Cst) == u32(Actor_Status.Stopped) {
		actor_drain_stopped_target(&target.ref.base, false)
	}
}

// Allocate an envelope from `pool`, copy `msg` into it, and send to `target`.
send_from_pool :: #force_inline proc(
	s: ^Scheduler,
	pool: ^Envelope_Pool($Msg),
	target: ^Actor($State, Msg),
	msg: Msg,
	shard_hint: int = -1,
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) { return }
	env := pool_acquire(pool)
	env.msg = msg
	hint := int(pool.pool_id) if shard_hint < 0 else shard_hint
	send_envelope(s, target, env, hint)
}

send_from_pool_pid :: #force_inline proc(
	s: ^Scheduler,
	pool: ^Envelope_Pool($Msg),
	target: PID(Msg),
	msg: Msg,
	shard_hint: int = -1,
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) { return }
	env := pool_acquire(pool)
	env.msg = msg
	hint := int(pool.pool_id) if shard_hint < 0 else shard_hint
	send_envelope_pid(s, target, env, hint)
}

// Allocate a chain of `len(msgs)` envelopes from `pool`, populate them with `msgs`,
// and splice the entire batch onto `target.mailbox` in ONE atomic `XCHG`!
send_batch_from_pool :: #force_inline proc(
	s: ^Scheduler,
	pool: ^Envelope_Pool($Msg),
	target: ^Actor($State, Msg),
	msgs: []Msg,
	shard_hint: int = -1,
) {
	if len(msgs) == 0 || target == nil || !actor_base_can_accept(&target.base, 0) { return }
	first, last := pool_acquire_chain(pool, msgs)
	hint := int(pool.pool_id) if shard_hint < 0 else shard_hint
	send_envelope_chain(s, target, first, last, hint)
}

send_batch_from_pool_pid :: #force_inline proc(
	s: ^Scheduler,
	pool: ^Envelope_Pool($Msg),
	target: PID(Msg),
	msgs: []Msg,
	shard_hint: int = -1,
) {
	if len(msgs) == 0 || target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) { return }
	first, last := pool_acquire_chain(pool, msgs)
	hint := int(pool.pool_id) if shard_hint < 0 else shard_hint
	send_envelope_chain_pid(s, target, first, last, hint)
}

// Auto-pooled send with explicit `s: ^Scheduler` (no `^Envelope_Pool` argument needed!)
send_sched_auto :: #force_inline proc(
	s: ^Scheduler,
	target: ^Actor($State, $Msg),
	msg: Msg,
	shard_hint: int = -1,
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) { return }
	env, hint := acquire_auto_envelope(s, &target.ref, shard_hint)
	env.msg = msg
	send_envelope(s, target, env, hint)
}

send_sched_auto_pid :: #force_inline proc(
	s: ^Scheduler,
	target: PID($Msg),
	msg: Msg,
	shard_hint: int = -1,
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) { return }
	env, hint := acquire_auto_envelope(s, target.ref, shard_hint)
	env.msg = msg
	send_envelope_pid(s, target, env, hint)
}

send_sched_batch_auto :: #force_inline proc(
	s: ^Scheduler,
	target: ^Actor($State, $Msg),
	msgs: []Msg,
	shard_hint: int = -1,
) {
	if len(msgs) == 0 || target == nil || !actor_base_can_accept(&target.base, 0) { return }
	first, last, hint := acquire_auto_envelope_chain(s, &target.ref, msgs, shard_hint)
	send_envelope_chain(s, target, first, last, hint)
}

send_sched_batch_auto_pid :: #force_inline proc(
	s: ^Scheduler,
	target: PID($Msg),
	msgs: []Msg,
	shard_hint: int = -1,
) {
	if len(msgs) == 0 || target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) { return }
	first, last, hint := acquire_auto_envelope_chain(s, target.ref, msgs, shard_hint)
	send_envelope_chain_pid(s, target, first, last, hint)
}

// Inside an actor's `receive` callback: send a new message to `target` using the
// current worker's pool when on the same scheduler, or external path across schedulers.
send_ctx :: #force_inline proc(
	ctx: ^Context,
	target: ^Actor($State, $Msg),
	msg: Msg,
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) { return }
	target_sched := resolve_target_scheduler(ctx.sched, &target.ref)
	if target_sched != ctx.sched {
		send_sched_auto(target_sched, target, msg, -1)
		return
	}
	wid := ctx.worker_id
	if wid >= 0 && wid + 1 < len(target.pools) {
		send_from_pool(target_sched, &target.pools[wid], target, msg, wid)
	} else {
		send_sched_auto(target_sched, target, msg, wid)
	}
}

send_ctx_pid :: #force_inline proc(
	ctx: ^Context,
	target: PID($Msg),
	msg: Msg,
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) { return }
	target_sched := resolve_target_scheduler(ctx.sched, target.ref)
	if target_sched != ctx.sched {
		send_sched_auto_pid(target_sched, target, msg, -1)
		return
	}
	wid := ctx.worker_id
	if wid >= 0 && wid + 1 < len(target.ref.pools) {
		send_from_pool_pid(target_sched, &target.ref.pools[wid], target, msg, wid)
	} else {
		send_sched_auto_pid(target_sched, target, msg, wid)
	}
}

send_ctx_batch :: #force_inline proc(
	ctx: ^Context,
	target: ^Actor($State, $Msg),
	msgs: []Msg,
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) { return }
	target_sched := resolve_target_scheduler(ctx.sched, &target.ref)
	if target_sched != ctx.sched {
		send_sched_batch_auto(target_sched, target, msgs, -1)
		return
	}
	wid := ctx.worker_id
	if wid >= 0 && wid + 1 < len(target.pools) {
		send_batch_from_pool(target_sched, &target.pools[wid], target, msgs, wid)
	} else {
		send_sched_batch_auto(target_sched, target, msgs, wid)
	}
}

send_ctx_batch_pid :: #force_inline proc(
	ctx: ^Context,
	target: PID($Msg),
	msgs: []Msg,
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) { return }
	target_sched := resolve_target_scheduler(ctx.sched, target.ref)
	if target_sched != ctx.sched {
		send_sched_batch_auto_pid(target_sched, target, msgs, -1)
		return
	}
	wid := ctx.worker_id
	if wid >= 0 && wid + 1 < len(target.ref.pools) {
		send_batch_from_pool_pid(target_sched, &target.ref.pools[wid], target, msgs, wid)
	} else {
		send_sched_batch_auto_pid(target_sched, target, msgs, wid)
	}
}

// Ambient 2-arg `actor.send(target, msg)` — automatically resolves `ctx` (if on a worker
// thread) or `sched` (if on main/external thread) and uses the shared `Envelope_Pool(Msg)`!
send_ambient :: #force_inline proc(
	target: ^Actor($State, $Msg),
	msg: Msg,
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) { return }
	if ctx := _tls_active_ctx; ctx != nil {
		send_ctx(ctx, target, msg)
		return
	}
	s := resolve_target_scheduler(nil, &target.ref)
	assert(s != nil, "No active Scheduler found for actor.send(target, msg)")
	send_sched_auto(s, target, msg)
}

send_ambient_pid :: #force_inline proc(
	target: PID($Msg),
	msg: Msg,
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) { return }
	if ctx := _tls_active_ctx; ctx != nil {
		send_ctx_pid(ctx, target, msg)
		return
	}
	s := resolve_target_scheduler(nil, target.ref)
	assert(s != nil, "No active Scheduler found for actor.send(pid, msg)")
	send_sched_auto_pid(s, target, msg)
}

send_ambient_batch :: #force_inline proc(
	target: ^Actor($State, $Msg),
	msgs: []Msg,
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) { return }
	if ctx := _tls_active_ctx; ctx != nil {
		send_ctx_batch(ctx, target, msgs)
		return
	}
	s := resolve_target_scheduler(nil, &target.ref)
	assert(s != nil, "No active Scheduler found for actor.send(target, msgs)")
	send_sched_batch_auto(s, target, msgs)
}

send_ambient_batch_pid :: #force_inline proc(
	target: PID($Msg),
	msgs: []Msg,
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) { return }
	if ctx := _tls_active_ctx; ctx != nil {
		send_ctx_batch_pid(ctx, target, msgs)
		return
	}
	s := resolve_target_scheduler(nil, target.ref)
	assert(s != nil, "No active Scheduler found for actor.send(pid, msgs)")
	send_sched_batch_auto_pid(s, target, msgs)
}

// -----------------------------------------------------------------------------
// Zero-Copy `iso` Message Forwarding (`actor.forward`)
// -----------------------------------------------------------------------------

@(private)
forward_ref_impl :: #force_inline proc(
	ctx:         ^Context,
	target_ref:  ^Actor_Ref($Msg),
	pid_gen:     u32,
	has_update:  bool,
	updated_msg: Msg,
) {
	assert(!ctx.forwarded && ctx.current_env != nil, "actor.forward called twice or outside active message turn")
	assert(ctx.current_msg_type == typeid_of(Msg), "actor.forward target message type mismatch")

	env := cast(^Envelope(Msg))ctx.current_env
	saved_reply_slot := ctx.current_reply_slot
	saved_reply_flags := ctx.current_flags & (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)
	// Preserve `reply_slot` and call/reply flags on the forwarded envelope while
	// disarming the forwarding context so subsequent `reply` or turn exit is a no-op.
	ctx.current_reply_slot = nil
	ctx.current_flags &~= (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)

	if target_ref == nil || !actor_base_can_accept(&target_ref.base, pid_gen) {
		if saved_reply_slot != nil && (saved_reply_flags & ENVELOPE_FLAG_CALL) != 0 {
			complete_call_slot(saved_reply_slot, CALL_STATE_FAULTED)
		}
		env.reply_slot = nil
		env.flags &~= (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)
		return
	}

	target_sched := resolve_target_scheduler(ctx.sched, target_ref)
	same_pool_space := ctx.current_pools_raw != nil && ctx.current_pools_raw == raw_data(target_ref.pools)
	if target_sched == ctx.sched && same_pool_space {
		ctx.forwarded = true
		ctx.current_env = nil
		if has_update {
			env.msg = updated_msg
		}
		env.reply_slot = saved_reply_slot
		env.flags = (env.flags &~ (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)) | saved_reply_flags
		mpsc_push(&target_ref.mailbox, env)
		scheduler_schedule_actor(target_sched, &target_ref.base, ctx.worker_id)
		if intrinsics.atomic_load_explicit(&target_ref.base.status, .Seq_Cst) == u32(Actor_Status.Stopped) {
			actor_drain_stopped_target(&target_ref.base, false)
		}
		return
	}

	// Cross-scheduler or mismatched-pool forward: move payload & reply metadata to
	// a target-owned envelope so the caller safely recycles `env` to its own pool.
	new_env, hint := acquire_auto_envelope(target_sched, target_ref, -1)
	new_env.msg = updated_msg if has_update else env.msg
	new_env.reply_slot = saved_reply_slot
	new_env.flags |= saved_reply_flags
	env.reply_slot = nil
	env.flags &~= (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)
	mpsc_push(&target_ref.mailbox, new_env)
	scheduler_schedule_actor(target_sched, &target_ref.base, hint)
	if intrinsics.atomic_load_explicit(&target_ref.base.status, .Seq_Cst) == u32(Actor_Status.Stopped) {
		actor_drain_stopped_target(&target_ref.base, false)
	}
}

// Zero-copy `iso` ownership transfer inside an actor's `receive` callback!
// Marks `ctx.forwarded = true` on the worker's stack so the caller actor does
// NOT recycle `current_env`, mutates its payload to `updated_msg`, and pushes
// the exact same memory node directly onto `target.mailbox` (zero allocation, zero free!).
forward_ctx :: #force_inline proc(
	ctx: ^Context,
	target: ^Actor($State, $Msg),
	updated_msg: Msg,
) {
	forward_ref_impl(ctx, &target.ref if target != nil else nil, 0, true, updated_msg)
}

forward_ctx_pid :: #force_inline proc(
	ctx: ^Context,
	target: PID($Msg),
	updated_msg: Msg,
) {
	forward_ref_impl(ctx, target.ref, target.generation, true, updated_msg)
}

// Forward the current message envelope with an updated payload using the ambient worker context.
forward_ambient :: #force_inline proc(
	target: ^Actor($State, $Msg),
	updated_msg: Msg,
) {
	ctx := current_context()
	assert(ctx != nil, "actor.forward can only be called inside an actor handler")
	forward_ctx(ctx, target, updated_msg)
}

forward_ambient_pid :: #force_inline proc(
	target: PID($Msg),
	updated_msg: Msg,
) {
	ctx := current_context()
	assert(ctx != nil, "actor.forward can only be called inside an actor handler")
	forward_ctx_pid(ctx, target, updated_msg)
}

// Forward the current message envelope as-is (when mutated in-place via `msg.field = ...`)
forward_unchanged_ctx :: #force_inline proc(
	ctx: ^Context,
	target: ^Actor($State, $Msg),
) {
	empty: Msg
	forward_ref_impl(ctx, &target.ref if target != nil else nil, 0, false, empty)
}

forward_unchanged_ctx_pid :: #force_inline proc(
	ctx: ^Context,
	target: PID($Msg),
) {
	empty: Msg
	forward_ref_impl(ctx, target.ref, target.generation, false, empty)
}

forward_unchanged_ambient :: #force_inline proc(
	target: ^Actor($State, $Msg),
) {
	ctx := current_context()
	assert(ctx != nil, "actor.forward can only be called inside an actor handler")
	forward_unchanged_ctx(ctx, target)
}

forward_unchanged_ambient_pid :: #force_inline proc(
	target: PID($Msg),
) {
	ctx := current_context()
	assert(ctx != nil, "actor.forward can only be called inside an actor handler")
	forward_unchanged_ctx_pid(ctx, target)
}

forward :: proc {
	forward_ctx,
	forward_ctx_pid,
	forward_ambient,
	forward_ambient_pid,
	forward_unchanged_ctx,
	forward_unchanged_ctx_pid,
	forward_unchanged_ambient,
	forward_unchanged_ambient_pid,
}

// -----------------------------------------------------------------------------
// Request / Reply (`actor.call`, `actor.reply`, & Envelope-Level `reply_to`)
// -----------------------------------------------------------------------------

// Send a message to `target` with `reply_to` attached in the envelope header
// (so `Msg` does not need a `reply_to` field!). When `target` calls `actor.reply(val)`,
// `val` is automatically delivered to `reply_to`'s mailbox.
send_with_reply_actor_pid :: #force_inline proc(
	target: ^Actor($State, $Msg),
	msg: Msg,
	reply_to: PID($Res),
) {
	if target == nil || !actor_base_can_accept(&target.base, 0) { return }
	s := resolve_target_scheduler(nil, &target.ref)
	assert(s != nil, "No active Scheduler found for actor.send(target, msg, reply_to)")
	env, hint := acquire_auto_envelope(s, &target.ref, -1)
	env.flags |= ENVELOPE_FLAG_REPLY_PID
	env.reply_slot = reply_to.ref
	env.msg = msg
	send_envelope(s, target, env, hint)
}

send_with_reply_pid_pid :: #force_inline proc(
	target: PID($Msg),
	msg: Msg,
	reply_to: PID($Res),
) {
	if target.ref == nil || !actor_base_can_accept(&target.ref.base, target.generation) { return }
	s := resolve_target_scheduler(nil, target.ref)
	assert(s != nil, "No active Scheduler found for actor.send(pid, msg, reply_to)")
	env, hint := acquire_auto_envelope(s, target.ref, -1)
	env.flags |= ENVELOPE_FLAG_REPLY_PID
	env.reply_slot = reply_to.ref
	env.msg = msg
	send_envelope_pid(s, target, env, hint)
}

send_with_reply_actor_actor :: #force_inline proc(
	target: ^Actor($State, $Msg),
	msg: Msg,
	reply_to: ^Actor($Reply_State, $Res),
) {
	send_with_reply_actor_pid(target, msg, pid(reply_to))
}

send_with_reply_pid_actor :: #force_inline proc(
	target: PID($Msg),
	msg: Msg,
	reply_to: ^Actor($Reply_State, $Res),
) {
	send_with_reply_pid_pid(target, msg, pid(reply_to))
}

@(private)
complete_call_slot :: #force_inline proc(slot_ptr: rawptr, final_state: u32) {
	if slot_ptr == nil { return }
	ready := cast(^sync.Futex)slot_ptr
	prev := intrinsics.atomic_exchange_explicit((^u32)(ready), CALL_STATE_SIGNALING, .Acq_Rel)
	if prev == CALL_STATE_PARKED {
		// Caller stays blocked in loop while state == CALL_STATE_SIGNALING, ensuring
		// `sync.futex_signal` completes before the stack frame can be popped.
		sync.futex_signal(ready)
	}
	intrinsics.atomic_store_explicit((^u32)(ready), final_state, .Release)
}

// Reply to the sender of the currently executing message (`actor.call` or `send(..., reply_to)`).
reply_ctx :: #force_inline proc(ctx: ^Context, val: $Res) {
	slot_ptr := ctx.current_reply_slot
	if slot_ptr == nil { return }
	flags := ctx.current_flags
	ctx.current_reply_slot = nil
	ctx.current_flags &~= (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)
	if ctx.current_env != nil {
		raw_env := cast(^Envelope(u8))ctx.current_env
		raw_env.reply_slot = nil
		raw_env.flags &~= (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)
	}
	if (flags & ENVELOPE_FLAG_CALL) != 0 {
		slot := cast(^Call_Slot(Res))slot_ptr
		assert(slot.res_type == typeid_of(Res), "actor.reply result type mismatch with actor.call")
		slot.result = val
		complete_call_slot(slot_ptr, CALL_STATE_REPLIED)
	} else if (flags & ENVELOPE_FLAG_REPLY_PID) != 0 {
		target_ref := cast(^Actor_Ref(Res))slot_ptr
		send_ctx_pid(ctx, PID(Res){ref = target_ref}, val)
	}
}

reply_ambient :: #force_inline proc(val: $Res) {
	ctx := current_context()
	assert(ctx != nil, "actor.reply can only be called inside an actor handler")
	reply_ctx(ctx, val)
}

reply :: proc {
	reply_ctx,
	reply_ambient,
}

// Cooperatively execute one runnable actor turn while a worker thread is waiting inside `actor.call`.
@(private)
scheduler_step_one_cooperative :: proc(s: ^Scheduler, caller_ctx: ^Context) -> bool {
	if _tls_call_depth >= MAX_COOPERATIVE_CALL_DEPTH {
		panic("actor.call cooperative recursion depth limit exceeded")
	}
	wid := caller_ctx.worker_id
	num_shards := s.num_shards
	a := shard_pop_or_steal(&s.shards[wid % num_shards])
	if a == nil && num_shards > 1 {
		for i := 1; i < num_shards; i += 1 {
			peer := (wid + i) % num_shards
			a = shard_pop_or_steal(&s.shards[peer])
			if a != nil { break }
		}
	}
	if a == nil { return false }

	saved_ctx := caller_ctx^
	saved_tls := _tls_active_ctx
	_tls_call_depth += 1
	defer {
		_tls_call_depth -= 1
		caller_ctx^ = saved_ctx
		_tls_active_ctx = saved_tls
	}

	processed, has_more := a.step_proc(a, caller_ctx, s.batch_quota)
	caller_ctx^ = saved_ctx

	if has_more {
		shard_push(&s.shards[wid % num_shards], a)
		if s.enable_parking {
			intrinsics.atomic_thread_fence(.Seq_Cst)
			if intrinsics.atomic_load_explicit(&s.parked_workers, .Acquire) > 0 {
				intrinsics.atomic_add_explicit((^u32)(&s.wake_futex), 1, .Release)
				sync.futex_signal(&s.wake_futex)
			}
		}
	} else {
		intrinsics.atomic_store_explicit(&a.scheduled, 0, .Seq_Cst)
		_, still_has_more := a.step_proc(a, caller_ctx, 0)
		if still_has_more {
			if intrinsics.atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0 {
				shard_push(&s.shards[wid % num_shards], a)
			} else {
				intrinsics.atomic_sub_explicit(&s.active_actors, 1, .Seq_Cst)
			}
		} else {
			intrinsics.atomic_sub_explicit(&s.active_actors, 1, .Seq_Cst)
		}
	}
	if processed > 0 {
		intrinsics.atomic_add_explicit(&s.total_processed, i64(processed), .Release)
	}
	return true
}

@(private)
wait_for_call_slot :: proc(s: ^Scheduler, slot: ^Call_Slot($Res), caller_ctx: ^Context) -> u32 {
	spin := 0
	for {
		state := intrinsics.atomic_load_explicit((^u32)(&slot.ready), .Acquire)
		if state >= CALL_STATE_REPLIED {
			return state
		}
		if caller_ctx != nil && caller_ctx.sched == s && s.num_workers == 1 {
			if state == CALL_STATE_PENDING {
				if !scheduler_step_one_cooperative(s, caller_ctx) {
					intrinsics.cpu_relax()
				}
			} else {
				intrinsics.cpu_relax()
			}
		} else {
			if state == CALL_STATE_SIGNALING || spin < DEFAULT_FUTEX_SPIN_LIMIT {
				spin += 1
				intrinsics.cpu_relax()
			} else {
				old_st, cas_ok := intrinsics.atomic_compare_exchange_strong_explicit(
					(^u32)(&slot.ready),
					CALL_STATE_PENDING,
					CALL_STATE_PARKED,
					.Acq_Rel,
					.Acquire,
				)
				if cas_ok || old_st == CALL_STATE_PARKED {
					sync.futex_wait(&slot.ready, CALL_STATE_PARKED)
				}
				spin = 0
			}
		}
	}
}

// Synchronous request/reply (`GenServer.call` / Akka `ask`): sends `msg` to `target`,
// waits for `target` to call `actor.reply(res)` (cooperatively stepping actors if called
// from a 1-worker thread), and returns `res` of type `$Res`.
@(require_results)
call_sched_pid :: proc(
	s: ^Scheduler,
	target: PID($Msg),
	msg: Msg,
	$Res: typeid,
) -> Res {
	if target.ref == nil {
		if is_trap_active() {
			fail_turn_reason(.Panic, "actor.call on nil PID")
		}
		assert(target.ref != nil, "actor.call on nil PID")
		return {}
	}
	caller_ctx := _tls_active_ctx
	if caller_ctx != nil && caller_ctx.sched == s {
		if caller_ctx.current_actor == &target.ref.base {
			panic("actor.call self-call deadlock detected")
		}
		if s.num_workers == 1 && intrinsics.atomic_load_explicit(&target.ref.base.in_step, .Seq_Cst) != 0 {
			panic("actor.call cycle detected on single-worker scheduler")
		}
		if _tls_call_depth >= MAX_COOPERATIVE_CALL_DEPTH {
			panic("actor.call cooperative recursion depth limit exceeded")
		}
	}

	slot := Call_Slot(Res){
		ready    = sync.Futex(CALL_STATE_PENDING),
		res_type = typeid_of(Res),
	}
	env, hint := acquire_auto_envelope(s, target.ref, -1)
	env.flags |= ENVELOPE_FLAG_CALL
	env.reply_slot = &slot
	env.msg = msg
	send_envelope_pid(s, target, env, hint)

	_ = wait_for_call_slot(s, &slot, caller_ctx)
	return slot.result
}

@(require_results)
call_sched_actor :: proc(
	s: ^Scheduler,
	target: ^Actor($State, $Msg),
	msg: Msg,
	$Res: typeid,
) -> Res {
	return call_sched_pid(s, pid(target), msg, Res)
}

@(require_results)
call_ambient_pid :: proc(
	target: PID($Msg),
	msg: Msg,
	$Res: typeid,
) -> Res {
	s := resolve_target_scheduler(nil, target.ref)
	assert(s != nil, "No active Scheduler found for actor.call(pid, msg, Res)")
	return call_sched_pid(s, target, msg, Res)
}

@(require_results)
call_ambient_actor :: proc(
	target: ^Actor($State, $Msg),
	msg: Msg,
	$Res: typeid,
) -> Res {
	s := resolve_target_scheduler(nil, &target.ref)
	assert(s != nil, "No active Scheduler found for actor.call(target, msg, Res)")
	return call_sched_pid(s, pid(target), msg, Res)
}

call :: proc {
	call_sched_pid,
	call_sched_actor,
	call_ambient_pid,
	call_ambient_actor,
}

