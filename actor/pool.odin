package actor

import "base:intrinsics"
import "core:mem"

// =============================================================================
// 1. Message Envelope & `snmalloc`-Style Batched Recycling Pool
//
// Solves cross-thread memory recycling without per-message cache line bouncing:
// 1. Each producer/worker allocates from its own local `Envelope_Pool($Msg)`
//    (either from its local bump slab or by draining its `remote_returns` queue
//    in a single atomic operation).
// 2. When an actor consumes a batch of messages (e.g. up to `batch_quota`),
//    all envelopes originating from the same pool are linked together locally
//    and returned in ONE `mpsc_push_chain` call!
// 3. If an actor calls `forward(ctx, next_actor, env)`, the envelope's
//    `ENVELOPE_FLAG_FORWARDED` bit is set so the scheduler does NOT recycle it
//    (zero-copy `iso` ownership transfer).
// =============================================================================

MAX_POOLS :: 32

ENVELOPE_FLAG_NONE      :: u16(0)
ENVELOPE_FLAG_FORWARDED :: u16(1 << 0) // Message was forwarded (`iso`); do not auto-recycle
ENVELOPE_FLAG_STATIC    :: u16(1 << 1) // Pre-allocated static/stack token; do not recycle to pool
ENVELOPE_FLAG_HEAP      :: u16(1 << 2) // Fallback heap-allocated node outside the contiguous slab
ENVELOPE_FLAG_CALL      :: u16(1 << 3) // Synchronous `actor.call` waiting on `reply_slot` (`^Call_Slot(Res)`)
ENVELOPE_FLAG_REPLY_PID :: u16(1 << 4) // Async `reply_to` `^Actor_Ref(Res)` stored in `reply_slot`

// Intrusive message wrapper linking user payload `$Msg` inside an `MPSC_Queue`.
Envelope :: struct($Msg: typeid) {
	next:       ^Envelope(Msg), // Offset 0: Intrusive MPSC link
	pool_id:    u16,            // ID of the owning `Envelope_Pool` (0 .. MAX_POOLS-1)
	flags:      u16,            // `ENVELOPE_FLAG_*`
	reply_slot: rawptr,         // Optional `^Call_Slot(Res)` or `^Actor_Ref(Res)` for `actor.reply`
	msg:        Msg,            // User payload
}

// Per-producer / per-worker message envelope pool with a contiguous primary slab
// and a lock-free `remote_returns` MPSC queue for cross-thread batched reclamation.
Envelope_Pool :: struct($Msg: typeid) #align(64) {
	// Producer-local fields (Cache Line 0 - zero cross-thread contention)
	local_free:    ^Envelope(Msg),
	slab:          []Envelope(Msg),
	slab_idx:      int,
	pool_id:       u16,
	heap_fallback: bool,
	_align_busy:   u8,
	busy:          u32,
	allocator:     mem.Allocator,
	_pad0:         [8]u8,

	// Cross-thread batched return queue (Cache Lines 1..3)
	// Consumers return chains of `[first .. last]` here once per batch quantum.
	remote_returns: MPSC_Queue(Envelope(Msg)),
}

// Initialize a thread-local `Envelope_Pool` with `slab_capacity` pre-allocated envelopes.
pool_init :: proc(
	p: ^Envelope_Pool($Msg),
	pool_id: u16,
	slab_capacity: int = 8192,
	allocator := context.allocator,
	heap_fallback: bool = false,
) -> mem.Allocator_Error {
	p.pool_id = pool_id
	p.heap_fallback = heap_fallback
	p.busy = 0
	p.allocator = allocator
	p.local_free = nil
	p.slab_idx = 0
	if slab_capacity > 0 {
		p.slab = make([]Envelope(Msg), slab_capacity, allocator) or_return
	}
	mpsc_init(&p.remote_returns)
	return .None
}

// Destroy an `Envelope_Pool` and free its backing slab.
pool_destroy :: proc(p: ^Envelope_Pool($Msg)) {
	if p.slab != nil {
		delete(p.slab, p.allocator)
		p.slab = nil
	}
}

// Allocate an envelope from the thread-local pool.
// Fast path: 1) pop from `local_free`, 2) bump-allocate from `slab`,
// 3) harvest returned messages from `remote_returns`, or 4) spin if bounded slab is full.
@(require_results)
pool_acquire :: #force_inline proc(p: ^Envelope_Pool($Msg), spin_if_exhausted: bool = true) -> ^Envelope(Msg) {
	// 1. Check thread-local freelist (zero atomics)
	if p.local_free != nil {
		env := p.local_free
		p.local_free = env.next
		env.next = nil
		env.pool_id = p.pool_id
		env.flags = ENVELOPE_FLAG_NONE
		return env
	}

	// 2. Bump-allocate from initial contiguous slab (zero atomics)
	if p.slab_idx < len(p.slab) {
		env := &p.slab[p.slab_idx]
		p.slab_idx += 1
		env.next = nil
		env.pool_id = p.pool_id
		env.flags = ENVELOPE_FLAG_NONE
		return env
	}

	// 3. Reclaim retired envelopes from our `remote_returns` MPSC queue
	for {
		env := mpsc_pop_detached(&p.remote_returns)
		if env != nil {
			// Drain up to 64 more into `local_free` to amortize queue checks
			for i := 0; i < 64; i += 1 {
				extra := mpsc_pop_detached(&p.remote_returns)
				if extra == nil do break
				extra.next = p.local_free
				p.local_free = extra
			}
			env.next = nil
			env.pool_id = p.pool_id
			env.flags = ENVELOPE_FLAG_NONE
			return env
		}

		if !spin_if_exhausted || p.heap_fallback {
			env_heap := new(Envelope(Msg), p.allocator)
			env_heap.next = nil
			env_heap.pool_id = p.pool_id
			env_heap.flags = ENVELOPE_FLAG_HEAP
			return env_heap
		}
		intrinsics.cpu_relax()
	}
}

// Acquire and pre-link a chain of `len(msgs)` envelopes from `p`, populating each
// envelope with the corresponding element of `msgs`. Returns `(first, last)` ready
// for `mpsc_push_chain` / `send_envelope_chain`.
@(require_results)
pool_acquire_chain :: #force_inline proc(
	p: ^Envelope_Pool($Msg),
	msgs: []Msg,
	spin_if_exhausted: bool = true,
) -> (first: ^Envelope(Msg), last: ^Envelope(Msg)) {
	assert(len(msgs) > 0, "msgs slice must be non-empty")
	first = pool_acquire(p, spin_if_exhausted)
	first.msg = msgs[0]
	prev := first
	for i := 1; i < len(msgs); i += 1 {
		cur := pool_acquire(p, spin_if_exhausted)
		cur.msg = msgs[i]
		intrinsics.atomic_store_explicit(&prev.next, cur, .Relaxed)
		prev = cur
	}
	return first, prev
}

// Batch-return collector used on the stack during an actor's step quantum.
// Groups retired envelopes by their `pool_id` and flushes each chain in ONE atomic XCHG.
Batch_Return_Collector :: struct($Msg: typeid) {
	first: [MAX_POOLS]^Envelope(Msg),
	last:  [MAX_POOLS]^Envelope(Msg),
}

// Add a retired envelope to the thread-local collector bucket for its `pool_id`.
collector_add :: #force_inline proc(c: ^Batch_Return_Collector($Msg), env: ^Envelope(Msg)) {
	pid := env.pool_id
	if pid >= MAX_POOLS do return
	env.next = nil
	if c.first[pid] == nil {
		c.first[pid] = env
		c.last[pid]  = env
	} else {
		intrinsics.atomic_store_explicit(&c.last[pid].next, env, .Relaxed)
		c.last[pid] = env
	}
}

// Flush all collected envelope chains back to their origin `Envelope_Pool`s.
// Each non-empty bucket is spliced into `pools[p].remote_returns` in a single `mpsc_push_chain`.
collector_flush :: #force_inline proc(
	c: ^Batch_Return_Collector($Msg),
	pools: []Envelope_Pool(Msg),
	current_wid: int = -1,
) {
	num_pools := len(pools)
	for p := 0; p < num_pools; p += 1 {
		first := c.first[p]
		if first == nil do continue
		last := c.last[p]
		if p == current_wid {
			last.next = pools[p].local_free
			pools[p].local_free = first
		} else {
			mpsc_push_chain(&pools[p].remote_returns, first, last)
		}
		c.first[p] = nil
		c.last[p]  = nil
	}
}

// =============================================================================
// 2. Single-Actor FIFO Ring Slab + Batched Watermark Retirement (Workloads 1 & 3)
//
// When a known set of producers feed a single actor (or each producer owns a
// dedicated ring slab for that actor), Pony's zero-RMW dummy-tail pop retires
// each producer's ring slots in strict FIFO order.
// =============================================================================

// Cache-line isolated counter tracking how many FIFO slab slots have been retired
// for a given producer.
Producer_Watermark :: struct #align(64) {
	retired_count: u64,
}

// Power-of-two ring slab of `Envelope($Msg)` slots retired in FIFO order via a
// `Producer_Watermark`.
Watermark_Slab :: struct($Msg: typeid, $N: int = 8192)
	where N > 0 && (N & (N - 1)) == 0 {
	slots:          []Envelope(Msg),
	cursor:         u64,
	cached_retired: u64,
	prod_id:        u16,
	allocator:      mem.Allocator,
}

// Initialize a `Watermark_Slab` for producer `prod_id`.
watermark_slab_init :: proc(
	s: ^Watermark_Slab($Msg, $N),
	prod_id: u16,
	allocator := context.allocator,
) -> mem.Allocator_Error {
	s.slots = make([]Envelope(Msg), N, allocator) or_return
	s.cursor = 0
	s.cached_retired = 0
	s.prod_id = prod_id
	s.allocator = allocator
	return .None
}

// Destroy a `Watermark_Slab` and free its slots slice.
watermark_slab_destroy :: proc(s: ^Watermark_Slab($Msg, $N)) {
	if s.slots != nil {
		delete(s.slots, s.allocator)
		s.slots = nil
	}
}

// Acquire the next FIFO slot from the producer's ring slab, spinning only when
// the ring is within 2 slots of wrapping around unretired messages.
@(require_results)
watermark_slab_acquire :: #force_inline proc(
	s: ^Watermark_Slab($Msg, $N),
	wm: ^Producer_Watermark,
) -> ^Envelope(Msg) {
	mask :: u64(N - 1)
	for s.cursor - s.cached_retired >= u64(N - 2) {
		s.cached_retired = intrinsics.atomic_load_explicit(&wm.retired_count, .Acquire)
		if s.cursor - s.cached_retired >= u64(N - 2) {
			intrinsics.cpu_relax()
		}
	}
	slot := &s.slots[s.cursor & mask]
	s.cursor += 1
	slot.pool_id = s.prod_id
	return slot
}

// Acquire and link a chain of `k` contiguous FIFO slots (`[first .. last]`) for
// `mpsc_push_chain` (`pony_sendv`).
@(require_results)
watermark_slab_acquire_chain :: #force_inline proc(
	s: ^Watermark_Slab($Msg, $N),
	wm: ^Producer_Watermark,
	k: int,
	init_msg: Msg,
) -> (first: ^Envelope(Msg), last: ^Envelope(Msg)) {
	mask :: u64(N - 1)
	for (s.cursor + u64(k)) - s.cached_retired >= u64(N - 2) {
		s.cached_retired = intrinsics.atomic_load_explicit(&wm.retired_count, .Acquire)
		if (s.cursor + u64(k)) - s.cached_retired >= u64(N - 2) {
			intrinsics.cpu_relax()
		}
	}
	first = &s.slots[s.cursor & mask]
	prev := first
	first.pool_id = s.prod_id
	first.msg = init_msg
	s.cursor += 1

	for i := 1; i < k; i += 1 {
		cur := &s.slots[s.cursor & mask]
		s.cursor += 1
		cur.pool_id = s.prod_id
		cur.msg = init_msg
		intrinsics.atomic_store_explicit(&prev.next, cur, .Relaxed)
		prev = cur
	}
	return first, prev
}
