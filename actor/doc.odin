/*
Package `actor` (Sleipnir — "Odin's Pony") provides a high-performance, lock-free
Pony-style M:N Actor runtime and concurrency primitives for Odin.

### Architecture Overview

1. **Intrusive Lock-Free MPSC Mailboxes (`MPSC_Queue`)**:
   Modeled after `libponyrt` and Vyukov's intrusive MPSC queue. Producers enqueue messages
   with a single atomic `XCHG` (`mpsc_push`) or splice an entire pre-linked batch of
   messages in one `XCHG` (`mpsc_push_chain` / `pony_sendv`). Consumers pop messages using
   either detached sentinel re-insertion (`mpsc_pop_detached`, allowing immediate pool
   recycling or zero-copy `iso` forwarding across actors) or Pony's zero-RMW dummy-tail
   pop (`mpsc_pop_dummy`).

2. **Wait-Free Sharded SPSC Ring Lanes (`SPSC_Lane` & `Sharded_SPSC_Mailbox`)**:
   For fixed-producer fan-in topologies, `Sharded_SPSC_Mailbox` assigns one cache-aligned
   ring buffer lane per producer thread, eliminating atomic read-modify-write (`lock xchg`)
   instructions completely in favor of pure `.Acquire` loads and `.Release` stores.

3. **`snmalloc`-Inspired Batched Envelope Recycling (`Envelope_Pool` & `Watermark_Slab`)**:
   Cross-thread per-message memory freeing is the primary bottleneck in traditional actor
   mailboxes due to MESI cache-line invalidation. `Envelope_Pool` eliminates per-message
   remote frees by collecting consumed message envelopes during an actor's batch quantum
   and splicing the entire retired chain back to its origin pool in a single atomic operation.

4. **Sharded Work-Stealing M:N Scheduler (`Scheduler` & `Actor($State, $Msg)`)**:
   Multiplexes thousands of lightweight, strongly-typed `Actor($State, $Msg)` instances
   onto a configurable pool of OS worker threads. Supports heterogeneous actor types on
   the same scheduler, zero-copy `iso` message forwarding (`actor.forward`), lock-free
   `.Seq_Cst` Dekker wake/unschedule barriers, and optional adaptive 2-phase `sync.Futex`
   worker parking (`enable_parking = true`) for 0% idle CPU utilization.
*/
package actor

// =============================================================================
// Idiomatic Odin Procedure Overload Groups
// =============================================================================

// Initialize an actor, scheduler, memory pool, watermark slab, or queue.
init :: proc {
	scheduler_init,
	actor_init,
	pool_init,
	watermark_slab_init,
	mpsc_init,
	shard_init,
	sharded_spsc_init,
}

// Destroy and free resources owned by a scheduler, actor, pool, or mailbox.
destroy :: proc {
	scheduler_stop_and_destroy,
	actor_destroy,
	pool_destroy,
	watermark_slab_destroy,
	sharded_spsc_destroy,
}

// Push a single message or node onto a queue or SPSC lane.
push :: proc {
	mpsc_push,
	shard_push,
	spsc_try_push,
	sharded_spsc_push,
}

// Send a message or envelope to a typed `Actor($State, $Msg)`.
send :: proc {
	send_ctx,
	send_from_pool,
	send_batch_from_pool,
	send_envelope,
	send_envelope_chain,
}
