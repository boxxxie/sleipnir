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

4. **Sharded Work-Stealing M:N Scheduler (`Scheduler`, `Actor($State, $Msg)`, & `PID($Msg)`)**:
   Multiplexes thousands of lightweight, strongly-typed `Actor($State, $Msg)` instances
   onto a configurable pool of OS worker threads. Supports heterogeneous actor types on
   the same scheduler, zero-copy `iso` message forwarding (`actor.forward`), lock-free
   `.Seq_Cst` Dekker wake/unschedule barriers, optional adaptive 2-phase `sync.Futex`
   worker parking (`enable_parking = true`), single-line `scoped_scheduler` (`@(deferred_out)`),
   ambient thread-local context, and automatic per-message-type `Envelope_Pool` management.
*/
package actor

// =============================================================================
// Idiomatic Odin Procedure Overload Groups
// =============================================================================

// Initialize an actor, scheduler, memory pool, watermark slab, or queue.
init :: proc {
	scheduler_init,
	scheduler_init_workers,
	actor_init,
	actor_init_state,
	actor_init_simple,
	actor_init_val,
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

// Send a message, batch slice, or envelope to a typed `^Actor($State, $Msg)` or `PID($Msg)`.
send :: proc {
	send_ctx,
	send_ctx_pid,
	send_ctx_batch,
	send_ctx_batch_pid,
	send_from_pool,
	send_from_pool_pid,
	send_batch_from_pool,
	send_batch_from_pool_pid,
	send_sched_auto,
	send_sched_auto_pid,
	send_sched_batch_auto,
	send_sched_batch_auto_pid,
	send_envelope,
	send_envelope_pid,
	send_envelope_chain,
	send_envelope_chain_pid,
	send_ambient,
	send_ambient_pid,
	send_ambient_batch,
	send_ambient_batch_pid,
	send_ambient_envelope,
	send_ambient_envelope_pid,
	send_with_reply_actor_pid,
	send_with_reply_pid_pid,
	send_with_reply_actor_actor,
	send_with_reply_pid_actor,
}
