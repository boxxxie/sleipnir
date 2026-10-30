package actor

import "base:intrinsics"
import "core:mem"
import "core:os"
import "core:sync"
import "core:thread"

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
// =============================================================================

DEFAULT_BATCH_QUOTA      :: 64
DEFAULT_FUTEX_SPIN_LIMIT :: 1024

// Intrusive base embedded at offset 0 of every concrete `Actor($State, $Msg)`.
// Allows a single `Scheduler` to queue and step heterogeneous actor types.
Actor_Base :: struct #align(64) {
	next:       ^Actor_Base, // Offset 0: Intrusive link for `RunQ_Shard(Actor_Base)`
	scheduled:  u32,         // Atomic flag: 0 = idle, 1 = queued/running on scheduler
	id:         u32,         // Optional actor ID for routing / debugging
	user_ptr:   rawptr,      // Optional user context pointer
	step_proc:  proc(base: ^Actor_Base, ctx: ^Context, quota: int) -> (processed: int, has_more: bool),
}

// Execution context passed into every actor's `receive` callback.
Context :: struct {
	sched:       ^Scheduler,
	worker_id:   int,
	current_env: rawptr, // Pointer to the active `^Envelope(Msg)` being processed
}

// Concrete typed Actor parameterized over its private `$State` and `$Msg` type.
Actor :: struct($State: typeid, $Msg: typeid) #align(64) {
	using base: Actor_Base,
	mailbox:    MPSC_Queue(Envelope(Msg)),
	state:      State,
	receive:    proc(ctx: ^Context, self: ^Actor(State, Msg), msg: ^Msg),
	pools:      []Envelope_Pool(Msg),
	allocator:  mem.Allocator,
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

	// Global coordination & idle detection (cache-line isolated)
	_pad0:             [64]u8,
	active_actors:     i64,        // Number of currently scheduled actors across all shards
	pending_tokens:    i64,        // Optional user token/task countdown for `scheduler_wait_tokens`
	total_processed:   i64,        // Optional message counter for `scheduler_wait_messages`
	target_messages:   i64,        // Target message count (0 = disabled)
	running:           b32,        // False when shutting down
	parked_workers:    u32,        // Count of workers sleeping on `wake_futex`
	wake_futex:        sync.Futex, // Futex signaled when an actor becomes runnable
}

// -----------------------------------------------------------------------------
// Scheduler Lifecycle
// -----------------------------------------------------------------------------

// Initialize a `Scheduler` with the given `Scheduler_Config` and `allocator`.
scheduler_init :: proc(
	s: ^Scheduler,
	cfg: Scheduler_Config = {},
	allocator := context.allocator,
) -> mem.Allocator_Error {
	s.allocator = allocator
	s.num_workers = cfg.num_workers if cfg.num_workers > 0 else max(1, os.get_processor_core_count())
	s.num_shards  = cfg.num_shards  if cfg.num_shards >= s.num_workers else s.num_workers
	s.batch_quota = cfg.batch_quota if cfg.batch_quota > 0 else DEFAULT_BATCH_QUOTA
	s.enable_parking = cfg.enable_parking
	s.started = false

	s.shards = make([]RunQ_Shard(Actor_Base), s.num_shards, allocator) or_return
	for i := 0; i < s.num_shards; i += 1 {
		shard_init(&s.shards[i])
	}

	s.workers     = make([]^thread.Thread, s.num_workers, allocator) or_return
	s.worker_ctxs = make([]Worker_Thread_Ctx, s.num_workers, allocator) or_return
	s.running     = true
	s.active_actors = 0
	s.pending_tokens = 0
	s.total_processed = 0
	s.target_messages = 0
	s.parked_workers = 0
	s.wake_futex = 0
	return .None
}

// Spawn and start the scheduler's OS worker threads.
scheduler_start :: proc(s: ^Scheduler) {
	if s.started do return
	s.started = true
	for w := 0; w < s.num_workers; w += 1 {
		s.worker_ctxs[w] = Worker_Thread_Ctx{
			sched     = s,
			worker_id = w,
		}
		s.workers[w] = thread.create(scheduler_worker_proc)
		s.workers[w].data = &s.worker_ctxs[w]
		thread.start(s.workers[w])
	}
}

// Stop all worker threads, join them, and free all scheduler memory.
scheduler_stop_and_destroy :: proc(s: ^Scheduler) {
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
}

// Schedule an `Actor_Base` onto shard `shard_hint` if it is not already scheduled.
// Uses a `.Seq_Cst` Test-and-Test-and-Set (TTAS) load before `atomic_exchange` so
// sending to an already-active actor keeps `a.scheduled` in Shared (S) L1 cache state
// without issuing a redundant `lock xchg`.
scheduler_schedule_actor :: #force_inline proc(s: ^Scheduler, a: ^Actor_Base, shard_hint: int = 0) {
	if intrinsics.atomic_load_explicit(&a.scheduled, .Seq_Cst) == 0 {
		if intrinsics.atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0 {
			intrinsics.atomic_add_explicit(&s.active_actors, 1, .Acq_Rel)
			idx := u32(shard_hint) % u32(s.num_shards)
			shard_push(&s.shards[idx], a)

			if s.enable_parking {
				if intrinsics.atomic_load_explicit(&s.parked_workers, .Acquire) > 0 {
					intrinsics.atomic_add_explicit((^u32)(&s.wake_futex), 1, .Release)
					sync.futex_signal(&s.wake_futex)
				}
			}
		}
	}
}

// Wait until all actors have drained their mailboxes and become idle (`active_actors == 0`).
scheduler_wait_idle :: proc(s: ^Scheduler) {
	for intrinsics.atomic_load_explicit(&s.active_actors, .Acquire) > 0 {
		intrinsics.cpu_relax()
	}
}

// Wait until `pending_tokens` drops to 0 (useful for autonomous Actor-to-Actor graphs).
scheduler_wait_tokens :: proc(s: ^Scheduler) {
	for intrinsics.atomic_load_explicit(&s.pending_tokens, .Acquire) > 0 {
		intrinsics.cpu_relax()
	}
}

// Wait until `total_processed` reaches `target_msgs`.
scheduler_wait_messages :: proc(s: ^Scheduler, target_msgs: i64) {
	for intrinsics.atomic_load_explicit(&s.total_processed, .Acquire) < target_msgs {
		intrinsics.cpu_relax()
	}
}

// Decrement the scheduler's active token counter (e.g. when a circulating message finishes its hops).
complete_token :: #force_inline proc(ctx: ^Context, count: i64 = 1) {
	if intrinsics.atomic_sub_explicit(&ctx.sched.pending_tokens, count, .Acq_Rel) == count {
		if ctx.sched.enable_parking {
			intrinsics.atomic_add_explicit((^u32)(&ctx.sched.wake_futex), 1, .Release)
			sync.futex_broadcast(&ctx.sched.wake_futex)
		}
	}
}

// -----------------------------------------------------------------------------
// Worker Loop (Sharded Run-Queue Pop + Work Stealing + Lock-Free Reschedule)
// -----------------------------------------------------------------------------

@(private)
scheduler_worker_proc :: proc(t: ^thread.Thread) {
	wctx := cast(^Worker_Thread_Ctx)t.data
	s    := wctx.sched
	wid  := wctx.worker_id
	ctx  := Context{
		sched     = s,
		worker_id = wid,
	}

	spin_count := 0
	num_shards := s.num_shards
	quota      := s.batch_quota

	for intrinsics.atomic_load_explicit(&s.running, .Acquire) {
		if s.target_messages > 0 &&
		   intrinsics.atomic_load_explicit(&s.total_processed, .Relaxed) >= s.target_messages {
			break
		}

		// 1. Pop from worker's home shard, then steal from peer shards
		a := shard_pop_or_steal(&s.shards[wid % num_shards])
		if a == nil && num_shards > 1 {
			for i := 1; i < num_shards; i += 1 {
				peer := (wid + i) % num_shards
				a = shard_pop_or_steal(&s.shards[peer])
				if a != nil do break
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
					intrinsics.atomic_add_explicit(&s.parked_workers, 1, .Acq_Rel)
					if intrinsics.atomic_load_explicit(&s.running, .Acquire) &&
					   intrinsics.atomic_load_explicit(&s.active_actors, .Acquire) == 0 {
						sync.futex_wait(&s.wake_futex, val)
					}
					intrinsics.atomic_sub_explicit(&s.parked_workers, 1, .Acq_Rel)
					spin_count = 0
				}
			}
			continue
		}

		spin_count = 0

		// 2. Execute up to `quota` messages on the actor
		processed, has_more := a.step_proc(a, &ctx, quota)
		if processed > 0 && s.target_messages > 0 {
			intrinsics.atomic_add_explicit(&s.total_processed, i64(processed), .Release)
		}

		// 3. Reschedule or unschedule with `.Seq_Cst` Dekker barrier to prevent lost wakeups
		if has_more {
			shard_push(&s.shards[wid % num_shards], a)
		} else {
			intrinsics.atomic_store_explicit(&a.scheduled, 0, .Seq_Cst)
			intrinsics.atomic_sub_explicit(&s.active_actors, 1, .Acq_Rel)
			// Re-check if a message arrived just before `scheduled = 0` became visible
			_, still_has_more := a.step_proc(a, &ctx, 0)
			if still_has_more {
				scheduler_schedule_actor(s, a, wid)
			}
		}
	}
}

// -----------------------------------------------------------------------------
// Typed Actor Initialization, Step Procedure, Sending, and Zero-Copy Forwarding
// -----------------------------------------------------------------------------

// Initialize a typed `Actor($State, $Msg)` in-place (ideal for contiguous slices of actors).
actor_init :: proc(
	a: ^Actor($State, $Msg),
	initial_state: State,
	receive_proc: proc(ctx: ^Context, self: ^Actor(State, Msg), msg: ^Msg),
	pools: []Envelope_Pool(Msg) = nil,
	id: u32 = 0,
	allocator := context.allocator,
) {
	a.base.next      = nil
	a.base.scheduled = 0
	a.base.id        = id
	a.base.step_proc = actor_generic_step_proc(State, Msg)
	mpsc_init(&a.mailbox)
	a.state     = initial_state
	a.receive   = receive_proc
	a.pools     = pools
	a.allocator = allocator
}

// Heap-allocate and initialize a typed `Actor($State, $Msg)`.
// Free with `actor.actor_destroy(a)` (or `actor.destroy(a)`).
@(require_results)
spawn :: proc(
	initial_state: $State,
	receive_proc: proc(ctx: ^Context, self: ^Actor(State, $Msg), msg: ^Msg),
	pools: []Envelope_Pool(Msg) = nil,
	id: u32 = 0,
	allocator := context.allocator,
) -> (a: ^Actor(State, Msg), err: mem.Allocator_Error) #optional_allocator_error {
	a = new(Actor(State, Msg), allocator) or_return
	actor_init(a, initial_state, receive_proc, pools, id, allocator)
	return a, .None
}

// Free a heap-allocated `Actor($State, $Msg)` created via `spawn`.
actor_destroy :: proc(a: ^Actor($State, $Msg)) {
	if a != nil {
		free(a, a.allocator)
	}
}

// Generates the specialized type-erased step procedure for `Actor($State, $Msg)`.
// When `quota == 0`, acts as a pure non-consuming readiness probe (`!mpsc_is_empty(&a.mailbox)`).
@(private)
actor_generic_step_proc :: proc($State: typeid, $Msg: typeid) -> proc(base: ^Actor_Base, ctx: ^Context, quota: int) -> (int, bool) {
	return proc(base: ^Actor_Base, ctx: ^Context, quota: int) -> (int, bool) {
		self := cast(^Actor(State, Msg))base
		if quota == 0 {
			return 0, !mpsc_is_empty(&self.mailbox)
		}

		collector: Batch_Return_Collector(Msg)
		has_pools := len(self.pools) > 0
		processed := 0

		for processed < quota {
			env := mpsc_pop_detached(&self.mailbox)
			if env == nil do break

			ctx.current_env = env
			self.receive(ctx, self, &env.msg)
			processed += 1

			if (env.flags & ENVELOPE_FLAG_FORWARDED) == 0 {
				if (env.flags & ENVELOPE_FLAG_HEAP) != 0 {
					pid := env.pool_id
					alloc := self.pools[pid].allocator if (has_pools && int(pid) < len(self.pools)) else self.allocator
					free(env, alloc)
				} else if has_pools && (env.flags & ENVELOPE_FLAG_STATIC) == 0 {
					collector_add(&collector, env)
				}
			}
		}

		if processed > 0 && has_pools {
			// Pass `-1` when external producers share `self.pools` so envelopes are always
			// returned via the thread-safe `remote_returns` MPSC queue.
			collector_flush(&collector, self.pools, -1)
		}

		return processed, !mpsc_is_empty(&self.mailbox)
	}
}

// Send a message to `target` using a pre-allocated or pooled `^Envelope(Msg)`.
send_envelope :: #force_inline proc(
	s: ^Scheduler,
	target: ^Actor($State, $Msg),
	env: ^Envelope(Msg),
	shard_hint: int = 0,
) {
	mpsc_push(&target.mailbox, env)
	scheduler_schedule_actor(s, &target.base, shard_hint)
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
	mpsc_push_chain(&target.mailbox, first, last)
	scheduler_schedule_actor(s, &target.base, shard_hint)
}

// Allocate an envelope from `pool`, copy `msg` into it, and send to `target`.
send_from_pool :: #force_inline proc(
	s: ^Scheduler,
	pool: ^Envelope_Pool($Msg),
	target: ^Actor($State, Msg),
	msg: Msg,
	shard_hint: int = -1,
) {
	env := pool_acquire(pool)
	env.msg = msg
	hint := int(pool.pool_id) if shard_hint < 0 else shard_hint
	send_envelope(s, target, env, hint)
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
	if len(msgs) == 0 do return
	first, last := pool_acquire_chain(pool, msgs)
	hint := int(pool.pool_id) if shard_hint < 0 else shard_hint
	send_envelope_chain(s, target, first, last, hint)
}

// Inside an actor's `receive` callback: send a new message to `target` using the
// current worker's pool (`target.pools[ctx.worker_id]`).
send_ctx :: #force_inline proc(
	ctx: ^Context,
	target: ^Actor($State, $Msg),
	msg: Msg,
) {
	wid := ctx.worker_id
	assert(wid >= 0 && wid < len(target.pools), "Actor has no Envelope_Pool configured for this worker_id")
	send_from_pool(ctx.sched, &target.pools[wid], target, msg, wid)
}

// Zero-copy `iso` ownership transfer inside an actor's `receive` callback!
// Marks the currently executing `Envelope(Msg)` as forwarded so the caller actor
// does NOT recycle it, mutates its payload to `updated_msg`, and pushes the exact
// same memory node directly onto `target.mailbox` (zero allocation, zero free!).
forward :: #force_inline proc(
	ctx: ^Context,
	target: ^Actor($State, $Msg),
	updated_msg: Msg,
) {
	env := cast(^Envelope(Msg))ctx.current_env
	env.flags |= ENVELOPE_FLAG_FORWARDED
	env.msg = updated_msg
	send_envelope(ctx.sched, target, env, ctx.worker_id)
}
