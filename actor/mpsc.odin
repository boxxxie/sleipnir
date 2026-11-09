package actor

import "base:intrinsics"
import "core:sync"

// =============================================================================
// 1. Intrusive Lock-Free MPSC Queue (Pony / Vyukov Hybrid)
//
// Any struct `T` pushed into `MPSC_Queue(T)` must have a field `next: ^T`
// ( typically at offset 0 via `using` or direct field definition).
//
// Provides two pop modes:
//   - `mpsc_pop_detached`: Re-inserts the embedded `stub` sentinel when the last
//     element is popped so the returned `^T` is 100% detached from the queue and
//     can be immediately recycled, freed, or forwarded (`iso`) to another actor.
//   - `mpsc_pop_dummy`: Leaves the last popped node in place as the new dummy
//     `tail` (zero consumer RMW atomics; ideal when nodes come from a dedicated
//     single-actor slab).
// =============================================================================

MPSC_Queue :: struct($T: typeid) #align(64) {
	head:  ^T,
	_pad0: [64 - size_of(rawptr)]u8,
	tail:  ^T,
	_pad1: [64 - size_of(rawptr)]u8,
	stub:  T,
}

mpsc_init :: proc(q: ^MPSC_Queue($T)) {
	q.stub.next = nil
	q.head = &q.stub
	q.tail = &q.stub
}

// Push a single node onto the tail of the MPSC queue (1 atomic XCHG + 1 Release store).
mpsc_push :: #force_inline proc(q: ^MPSC_Queue($T), node: ^T) {
	intrinsics.atomic_store_explicit(&node.next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&q.head, node, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.next, node, .Release)
}

// Splice a pre-linked chain `[first .. last]` onto the MPSC queue in a single atomic XCHG!
// Equivalent to `libponyrt`'s `pony_sendv` / `snmalloc` batch return.
mpsc_push_chain :: #force_inline proc(q: ^MPSC_Queue($T), first: ^T, last: ^T) {
	intrinsics.atomic_store_explicit(&last.next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&q.head, last, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.next, first, .Release)
}

// Check if the queue currently has no elements ready for immediate pop.
// In a Vyukov detached MPSC queue, the queue is empty when `head == tail` AND
// when `tail == &stub`, `stub.next == nil`.
@(require_results)
mpsc_is_empty :: #force_inline proc(q: ^MPSC_Queue($T)) -> bool {
	head := intrinsics.atomic_load_explicit(&q.head, .Seq_Cst)
	tail := intrinsics.atomic_load_explicit(&q.tail, .Relaxed)
	if head != tail {
		return false
	}
	if tail == &q.stub {
		return intrinsics.atomic_load_explicit(&q.stub.next, .Seq_Cst) == nil
	}
	return false
}

// Detached Vyukov MPSC pop: returns a completely detached `^T` that is no longer
// referenced by `q.tail`. Safe for multi-actor `iso` forwarding and pool recycling.
mpsc_pop_detached :: #force_inline proc(q: ^MPSC_Queue($T)) -> ^T {
	tail := q.tail
	next := intrinsics.atomic_load_explicit(&tail.next, .Acquire)

	if tail == &q.stub {
		if next == nil {
			return nil
		}
		q.tail = next
		tail = next
		next = intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	}

	if next != nil {
		q.tail = next
		return tail
	}

	head := intrinsics.atomic_load_explicit(&q.head, .Acquire)
	if tail != head {
		// Producer is mid-push between `XCHG` and `prev.next = node`
		return nil
	}

	// Re-insert the embedded `stub` sentinel so `tail` can be detached
	intrinsics.atomic_store_explicit(&q.stub.next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&q.head, &q.stub, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.next, &q.stub, .Release)

	next = intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	for next == nil {
		intrinsics.cpu_relax()
		next = intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	}
	q.tail = next
	return tail
}

// Pony-style dummy-tail pop: zero consumer RMW atomics.
// Returns `(new_tail_node, previous_dummy_tail_node, ok)`.
// Note: `new_tail_node` remains in `q.tail` as the sentinel until the NEXT message
// is popped, whereas `previous_dummy_tail_node` is now completely retired!
mpsc_pop_dummy :: #force_inline proc(q: ^MPSC_Queue($T)) -> (msg: ^T, retired_stub: ^T, ok: bool) {
	tail := q.tail
	next := intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	if next == nil {
		return nil, nil, false
	}
	q.tail = next
	return next, tail, true
}

// =============================================================================
// 2. Work-Stealing MPMC Run-Queue Shard (MPSC Push + Ticket-Locked Pop/Steal)
// =============================================================================

RunQ_Shard :: struct($T: typeid) #align(64) {
	using q:  MPSC_Queue(T),
	pop_lock: sync.Ticket_Mutex,
}

shard_init :: proc(s: ^RunQ_Shard($T)) {
	mpsc_init(&s.q)
}

shard_push :: #force_inline proc(s: ^RunQ_Shard($T), item: ^T) {
	mpsc_push(&s.q, item)
}

shard_pop_or_steal :: proc(s: ^RunQ_Shard($T)) -> ^T {
	// Fast lock-free check before acquiring ticket lock
	if mpsc_is_empty(&s.q) {
		return nil
	}
	sync.ticket_mutex_lock(&s.pop_lock)
	defer sync.ticket_mutex_unlock(&s.pop_lock)

	return mpsc_pop_detached(&s.q)
}
