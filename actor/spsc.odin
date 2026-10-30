package actor

import "base:intrinsics"
import "core:mem"

// =============================================================================
// Cache-Aligned Wait-Free SPSC Ring Buffer Lane & Sharded SPSC Mailbox
//
// Zero atomic Read-Modify-Write (`lock xchg` / `cmpxchg`) instructions!
// Uses pure `.Acquire` loads and `.Release` stores with cached indices to avoid
// cross-core cache line bouncing.
// =============================================================================

DEFAULT_SPSC_CAPACITY :: 8192

// A wait-free Single-Producer Single-Consumer ring buffer lane with 64-byte cache-line
// isolation between producer write indices, consumer read indices, and the data ring.
SPSC_Lane :: struct($T: typeid, $N: int = DEFAULT_SPSC_CAPACITY) #align(64)
	where N > 0 && (N & (N - 1)) == 0 {
	write_idx:        u64,
	_pad0:            [64 - size_of(u64)]u8,
	read_idx:         u64,
	cached_write_idx: u64,
	_pad1:            [64 - 2 * size_of(u64)]u8,
	cached_read_idx:  u64,
	_pad2:            [64 - size_of(u64)]u8,
	ring:             [N]T,
}

// Attempt to enqueue `val` into `lane` without blocking. Returns `false` if full.
@(require_results)
spsc_try_push :: #force_inline proc(lane: ^SPSC_Lane($T, $N), val: T) -> bool {
	mask :: u64(N - 1)
	w := intrinsics.atomic_load_explicit(&lane.write_idx, .Relaxed)
	if w - lane.cached_read_idx >= u64(N) {
		lane.cached_read_idx = intrinsics.atomic_load_explicit(&lane.read_idx, .Acquire)
		if w - lane.cached_read_idx >= u64(N) {
			return false
		}
	}
	lane.ring[w & mask] = val
	intrinsics.atomic_store_explicit(&lane.write_idx, w + 1, .Release)
	return true
}

// Enqueue `val` into `lane`, spinning with `cpu_relax()` if the ring is full.
spsc_push_spin :: #force_inline proc(lane: ^SPSC_Lane($T, $N), val: T) {
	for !spsc_try_push(lane, val) {
		intrinsics.cpu_relax()
	}
}

// Attempt to dequeue a single item from `lane`. Returns `(val, false)` if empty.
@(require_results)
spsc_try_pop :: #force_inline proc(lane: ^SPSC_Lane($T, $N)) -> (val: T, ok: bool) {
	mask :: u64(N - 1)
	r := intrinsics.atomic_load_explicit(&lane.read_idx, .Relaxed)
	if r == lane.cached_write_idx {
		lane.cached_write_idx = intrinsics.atomic_load_explicit(&lane.write_idx, .Acquire)
		if r == lane.cached_write_idx {
			return {}, false
		}
	}
	val = lane.ring[r & mask]
	intrinsics.atomic_store_explicit(&lane.read_idx, r + 1, .Release)
	return val, true
}

// Drain up to `max_batch` items from `lane` into `handler` with a single `.Release`
// update to `read_idx` at the end of the batch.
spsc_consume_batch :: #force_inline proc(
	lane: ^SPSC_Lane($T, $N),
	max_batch: int,
	state: ^$S,
	handler: proc(state: ^S, msg: ^T),
) -> int {
	mask :: u64(N - 1)
	r := intrinsics.atomic_load_explicit(&lane.read_idx, .Relaxed)
	w := lane.cached_write_idx
	if w - r < u64(max_batch) {
		w = intrinsics.atomic_load_explicit(&lane.write_idx, .Acquire)
		lane.cached_write_idx = w
	}
	avail := w - r
	if avail == 0 {
		return 0
	}
	if avail > u64(max_batch) {
		avail = u64(max_batch)
	}
	for k: u64 = 0; k < avail; k += 1 {
		handler(state, &lane.ring[(r + k) & mask])
	}
	intrinsics.atomic_store_explicit(&lane.read_idx, r + avail, .Release)
	return int(avail)
}

// =============================================================================
// Sharded SPSC Multi-Producer Mailbox (1 Wait-Free SPSC Lane Per Producer)
// =============================================================================

// Multi-producer mailbox that eliminates `lock xchg` contention by giving each
// producer thread its own dedicated cache-aligned `SPSC_Lane`.
Sharded_SPSC_Mailbox :: struct($T: typeid, $N: int = DEFAULT_SPSC_CAPACITY) #align(64)
	where N > 0 && (N & (N - 1)) == 0 {
	lanes:     []SPSC_Lane(T, N),
	allocator: mem.Allocator,
}

// Initialize a `Sharded_SPSC_Mailbox` with `num_producers` independent SPSC lanes.
sharded_spsc_init :: proc(
	m: ^Sharded_SPSC_Mailbox($T, $N),
	num_producers: int,
	allocator := context.allocator,
) -> mem.Allocator_Error {
	m.allocator = allocator
	m.lanes = make([]SPSC_Lane(T, N), num_producers, allocator) or_return
	return .None
}

// Destroy a `Sharded_SPSC_Mailbox` and free its lane array.
sharded_spsc_destroy :: proc(m: ^Sharded_SPSC_Mailbox($T, $N)) {
	if m.lanes != nil {
		delete(m.lanes, m.allocator)
		m.lanes = nil
	}
}

// Push `val` from producer `producer_id` into its dedicated wait-free SPSC lane.
sharded_spsc_push :: #force_inline proc(
	m: ^Sharded_SPSC_Mailbox($T, $N),
	producer_id: int,
	val: T,
) {
	spsc_push_spin(&m.lanes[producer_id], val)
}

// Poll all producer lanes round-robin, draining up to `quota_per_lane` messages
// per lane into `handler`. Returns the total number of messages processed across all lanes.
sharded_spsc_consume :: #force_inline proc(
	m: ^Sharded_SPSC_Mailbox($T, $N),
	quota_per_lane: int,
	state: ^$S,
	handler: proc(state: ^S, msg: ^T),
) -> int {
	total := 0
	for i := 0; i < len(m.lanes); i += 1 {
		total += spsc_consume_batch(&m.lanes[i], quota_per_lane, state, handler)
	}
	return total
}
