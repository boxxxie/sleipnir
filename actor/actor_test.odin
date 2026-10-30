package actor

import "base:intrinsics"
import "core:mem"
import "core:testing"

// =============================================================================
// Test Utilities
// =============================================================================

// Runs a test procedure inside a `mem.Tracking_Allocator` and asserts that
// zero bytes were leaked and zero invalid frees occurred.
@(private = "file")
with_leak_check :: proc(t: ^testing.T, body: proc(t: ^testing.T)) {
	track: mem.Tracking_Allocator
	mem.tracking_allocator_init(&track, context.allocator)
	defer mem.tracking_allocator_destroy(&track)

	saved_allocator := context.allocator
	context.allocator = mem.tracking_allocator(&track)
	body(t)
	context.allocator = saved_allocator

	testing.expect_value(t, len(track.allocation_map), 0)
	testing.expect_value(t, len(track.bad_free_array), 0)
}

@(private = "file")
Test_Node :: struct {
	next: ^Test_Node,
	val:  int,
}

// =============================================================================
// 1. Lock-Free MPSC Queue & Run-Queue Shard Tests (`mpsc.odin`)
// =============================================================================

@(test)
test_01_mpsc_single_push_and_detached_pop :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		q: MPSC_Queue(Test_Node)
		init(&q)
		testing.expect(t, mpsc_is_empty(&q))

		n1 := Test_Node{val = 10}
		n2 := Test_Node{val = 20}
		n3 := Test_Node{val = 30}

		push(&q, &n1)
		push(&q, &n2)
		push(&q, &n3)
		testing.expect(t, !mpsc_is_empty(&q))

		testing.expect_value(t, mpsc_pop_detached(&q).val, 10)
		testing.expect_value(t, mpsc_pop_detached(&q).val, 20)
		testing.expect_value(t, mpsc_pop_detached(&q).val, 30)

		testing.expect(t, mpsc_is_empty(&q))
		testing.expect(t, mpsc_pop_detached(&q) == nil)
	})
}

@(test)
test_02_mpsc_batch_chain_splice :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		q: MPSC_Queue(Test_Node)
		init(&q)

		// Pre-link a chain of 4 nodes: 1 -> 2 -> 3 -> 4
		chain := [4]Test_Node{
			{val = 1},
			{val = 2},
			{val = 3},
			{val = 4},
		}
		for i := 0; i < 3; i += 1 {
			chain[i].next = &chain[i + 1]
		}

		// Splice all 4 nodes onto the queue in a single atomic XCHG
		mpsc_push_chain(&q, &chain[0], &chain[3])

		for expected := 1; expected <= 4; expected += 1 {
			popped := mpsc_pop_detached(&q)
			testing.expect(t, popped != nil)
			testing.expect_value(t, popped.val, expected)
		}
		testing.expect(t, mpsc_is_empty(&q))
	})
}

@(test)
test_03_mpsc_dummy_tail_pop :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		q: MPSC_Queue(Test_Node)
		init(&q)
		q.stub.val = -1

		n1 := Test_Node{val = 100}
		n2 := Test_Node{val = 200}
		push(&q, &n1)
		push(&q, &n2)

		// Pop 1: returns `n1` as the message and retires the initial `stub` (-1)
		msg1, retired1, ok1 := mpsc_pop_dummy(&q)
		testing.expect(t, ok1)
		testing.expect_value(t, msg1.val, 100)
		testing.expect_value(t, retired1.val, -1)

		// Pop 2: returns `n2` as the message and retires `n1` (100)
		msg2, retired2, ok2 := mpsc_pop_dummy(&q)
		testing.expect(t, ok2)
		testing.expect_value(t, msg2.val, 200)
		testing.expect_value(t, retired2.val, 100)

		// Queue is now empty (`n2` remains as the new dummy tail sentinel)
		_, _, ok3 := mpsc_pop_dummy(&q)
		testing.expect(t, !ok3)
	})
}

@(test)
test_04_runq_shard_stealing :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		shard: RunQ_Shard(Test_Node)
		init(&shard)

		// Empty shard returns nil on fast lock-free path
		testing.expect(t, shard_pop_or_steal(&shard) == nil)

		n1 := Test_Node{val = 7}
		n2 := Test_Node{val = 8}
		push(&shard, &n1)
		push(&shard, &n2)

		testing.expect_value(t, shard_pop_or_steal(&shard).val, 7)
		testing.expect_value(t, shard_pop_or_steal(&shard).val, 8)
		testing.expect(t, shard_pop_or_steal(&shard) == nil)
	})
}

// =============================================================================
// 2. Wait-Free SPSC Lane & Sharded SPSC Mailbox Tests (`spsc.odin`)
// =============================================================================

@(test)
test_05_spsc_lane_ring_wraparound :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		lane := new(SPSC_Lane(int, 4))
		defer free(lane)

		// 1. Fill the 4-slot ring buffer to capacity
		testing.expect(t, spsc_try_push(lane, 10))
		testing.expect(t, spsc_try_push(lane, 20))
		testing.expect(t, spsc_try_push(lane, 30))
		testing.expect(t, spsc_try_push(lane, 40))
		testing.expect(t, !spsc_try_push(lane, 999)) // Full!

		// 2. Pop 2 items (`10` and `20`) to free 2 slots
		v1, ok1 := spsc_try_pop(lane)
		v2, ok2 := spsc_try_pop(lane)
		testing.expect(t, ok1 && ok2)
		testing.expect_value(t, v1, 10)
		testing.expect_value(t, v2, 20)

		// 3. Push 2 more items (`50` and `60`) across the ring wrap-around boundary
		spsc_push_spin(lane, 50)
		spsc_push_spin(lane, 60)

		// 4. Consume the remaining 4 items (`30, 40, 50, 60`) in a single batch
		sum := 0
		count := spsc_consume_batch(lane, 10, &sum, proc(total: ^int, item: ^int) {
			total^ += item^
		})
		testing.expect_value(t, count, 4)
		testing.expect_value(t, sum, 30 + 40 + 50 + 60)

		// 5. Lane is now empty
		_, ok_empty := spsc_try_pop(lane)
		testing.expect(t, !ok_empty)
		testing.expect_value(t, spsc_consume_batch(lane, 10, &sum, proc(total: ^int, item: ^int) {}), 0)
	})
}

@(test)
test_06_sharded_spsc_mailbox :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		mbox: Sharded_SPSC_Mailbox(int, 16)
		init(&mbox, num_producers = 3)
		defer destroy(&mbox)

		// Producer 0 pushes 1, Producer 1 pushes 10, Producer 2 pushes 100
		push(&mbox, 0, 1)
		push(&mbox, 0, 1)
		push(&mbox, 1, 10)
		push(&mbox, 2, 100)

		total := 0
		consumed := sharded_spsc_consume(&mbox, 8, &total, proc(acc: ^int, msg: ^int) {
			acc^ += msg^
		})
		testing.expect_value(t, consumed, 4)
		testing.expect_value(t, total, 112)
	})
}

// =============================================================================
// 3. Envelope Pool & Watermark Slab Tests (`pool.odin`)
// =============================================================================

@(test)
test_07_envelope_pool_and_batch_collector :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		pools := make([]Envelope_Pool(int), 2)
		init(&pools[0], pool_id = 0, slab_capacity = 4)
		init(&pools[1], pool_id = 1, slab_capacity = 4)
		defer {
			destroy(&pools[0])
			destroy(&pools[1])
			delete(pools)
		}

		// 1. Exhaust all 4 slab slots in Pool 0 & Pool 1 and collect them
		collector: Batch_Return_Collector(int)
		for i := 0; i < 4; i += 1 {
			e0 := pool_acquire(&pools[0])
			e1 := pool_acquire(&pools[1])
			collector_add(&collector, e0)
			collector_add(&collector, e1)
		}

		// 2. Test heap fallback when `spin_if_exhausted = false` and slab is empty
		heap_env := pool_acquire(&pools[0], spin_if_exhausted = false)
		testing.expect(t, (heap_env.flags & ENVELOPE_FLAG_HEAP) != 0)
		free(heap_env, pools[0].allocator)

		// 3. Flush Pool 0 as local (`current_wid = 0`) and Pool 1 as remote (`remote_returns`)
		collector_flush(&collector, pools, current_wid = 0)
		testing.expect(t, pools[0].local_free != nil)

		// 4. Re-acquire 4 envelopes from Pool 0 (from `local_free`) and Pool 1 (from `remote_returns`)
		batch_vals := [4]int{10, 20, 30, 40}
		first0, last0 := pool_acquire_chain(&pools[0], batch_vals[:])
		first1, last1 := pool_acquire_chain(&pools[1], batch_vals[:])
		testing.expect_value(t, first0.msg, 10)
		testing.expect_value(t, last0.msg, 40)
		testing.expect_value(t, first1.msg, 10)
		testing.expect_value(t, last1.msg, 40)
	})
}

@(test)
test_08_watermark_slab_fifo_and_chain :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		wm: Producer_Watermark
		slab: Watermark_Slab(int, 8)
		init(&slab, prod_id = 0)
		defer destroy(&slab)

		// Acquire 6 slots (out of 8) one-by-one
		for i := 0; i < 6; i += 1 {
			slot := watermark_slab_acquire(&slab, &wm)
			slot.msg = i
		}

		// Advance the consumer watermark by 6 so the 8-slot FIFO ring can wrap around
		intrinsics.atomic_add_explicit(&wm.retired_count, 6, .Release)

		// Acquire a pre-linked chain of 4 slots across the wrap-around boundary
		first, last := watermark_slab_acquire_chain(&slab, &wm, 4, 99)
		testing.expect_value(t, first.msg, 99)
		testing.expect_value(t, last.msg, 99)
	})
}

// =============================================================================
// 4. Actor & Work-Stealing M:N Scheduler Tests (`actor.odin`)
// =============================================================================

@(private = "file")
Counter_State :: struct {
	total:    i64,
	messages: int,
}

@(private = "file")
counter_receive :: proc(ctx: ^Context, self: ^Actor(Counter_State, i64), msg: ^i64) {
	self.state.total += msg^
	self.state.messages += 1
}

@(test)
test_09_actor_basic_counter :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sched: Scheduler
		init(&sched, Scheduler_Config{num_workers = 2})
		defer destroy(&sched)

		pools := make([]Envelope_Pool(i64), 1)
		init(&pools[0], 0, 32)
		defer {
			destroy(&pools[0])
			delete(pools)
		}

		counter := spawn(Counter_State{}, counter_receive, pools)
		defer destroy(counter)

		scheduler_start(&sched)

		// Send numbers 1..10 to `counter`
		for i: i64 = 1; i <= 10; i += 1 {
			send(&sched, &pools[0], counter, i)
		}

		scheduler_wait_messages(&sched, 10)
		scheduler_wait_idle(&sched)

		testing.expect_value(t, counter.state.messages, 10)
		testing.expect_value(t, counter.state.total, 55)
	})
}

@(test)
test_10_actor_batch_chain_send :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sched: Scheduler
		init(&sched, Scheduler_Config{num_workers = 1})
		defer destroy(&sched)

		pools := make([]Envelope_Pool(i64), 1)
		init(&pools[0], 0, 32)
		defer {
			destroy(&pools[0])
			delete(pools)
		}

		counter := spawn(Counter_State{}, counter_receive, pools)
		defer destroy(counter)

		scheduler_start(&sched)

		// Send 4 batches of 5 messages each (20 messages total) via `send_batch_from_pool`
		batch := [5]i64{2, 4, 6, 8, 10} // Sum = 30 per batch
		for b := 0; b < 4; b += 1 {
			send(&sched, &pools[0], counter, batch[:])
		}

		scheduler_wait_messages(&sched, 20)
		scheduler_wait_idle(&sched)

		testing.expect_value(t, counter.state.messages, 20)
		testing.expect_value(t, counter.state.total, 120)
	})
}

@(private = "file")
Double_Request :: struct {
	number:   i64,
	reply_to: ^Actor(Counter_State, i64),
}

@(private = "file")
Doubler_State :: struct {
	requests_handled: int,
}

@(test)
test_11_actor_heterogeneous_pipeline_with_futex_parking :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sched: Scheduler
		init(&sched, Scheduler_Config{
			num_workers    = 2,
			enable_parking = true, // Exercise adaptive 2-phase Futex sleep/wake
		})
		defer destroy(&sched)

		req_pools   := make([]Envelope_Pool(Double_Request), 2)
		reply_pools := make([]Envelope_Pool(i64), 2)
		for w := 0; w < 2; w += 1 {
			init(&req_pools[w], u16(w), 32)
			init(&reply_pools[w], u16(w), 32)
		}
		defer {
			for w := 0; w < 2; w += 1 {
				destroy(&req_pools[w])
				destroy(&reply_pools[w])
			}
			delete(req_pools)
			delete(reply_pools)
		}

		// Actor 1: Doubles incoming numbers and sends the result to `msg.reply_to`
		doubler := spawn(
			Doubler_State{},
			proc(ctx: ^Context, self: ^Actor(Doubler_State, Double_Request), msg: ^Double_Request) {
				self.state.requests_handled += 1
				send(ctx, msg.reply_to, msg.number * 2)
			},
			req_pools,
		)
		defer destroy(doubler)

		// Actor 2: Sums the doubled replies and decrements `sched.pending_tokens`
		aggregator := spawn(
			Counter_State{},
			proc(ctx: ^Context, self: ^Actor(Counter_State, i64), msg: ^i64) {
				self.state.total += msg^
				self.state.messages += 1
				complete_token(ctx, 1)
			},
			reply_pools,
		)
		defer destroy(aggregator)

		sched.pending_tokens = 20
		scheduler_start(&sched)

		for i: i64 = 1; i <= 20; i += 1 {
			send(&sched, &req_pools[0], doubler, Double_Request{
				number   = i,
				reply_to = aggregator,
			})
		}

		scheduler_wait_tokens(&sched)
		scheduler_wait_idle(&sched)

		testing.expect_value(t, doubler.state.requests_handled, 20)
		testing.expect_value(t, aggregator.state.messages, 20)
		// Sum of 2 * (1..20) = 20 * 21 = 420
		testing.expect_value(t, aggregator.state.total, 420)
	})
}

@(private = "file")
Stage_State :: struct {
	next_stage: ^Actor(Stage_State, i64),
	sum:        i64,
	count:      int,
}

@(test)
test_12_actor_zero_copy_forward_returns_to_pool :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sched: Scheduler
		init(&sched, Scheduler_Config{
			num_workers = 2,
			batch_quota = 4,
		})
		defer destroy(&sched)

		// Allocate a tiny 4-slot pool to prove that envelopes forwarded through
		// Stage 1 -> Stage 2 -> Sink are properly recycled back to the pool by Sink!
		pools := make([]Envelope_Pool(i64), 1)
		init(&pools[0], 0, slab_capacity = 4)
		defer {
			destroy(&pools[0])
			delete(pools)
		}

		stage_receive :: proc(ctx: ^Context, self: ^Actor(Stage_State, i64), msg: ^i64) {
			self.state.count += 1
			if self.state.next_stage != nil {
				// Forward the exact same Envelope(i64) in-place (`iso`) with +10 added
				forward(ctx, self.state.next_stage, msg^ + 10)
			} else {
				// Final sink stage: does NOT forward, so `step_proc` recycles the envelope!
				self.state.sum += msg^
				complete_token(ctx, 1)
			}
		}

		sink   := spawn(Stage_State{next_stage = nil},    stage_receive, pools)
		stage2 := spawn(Stage_State{next_stage = sink},   stage_receive, pools)
		stage1 := spawn(Stage_State{next_stage = stage2}, stage_receive, pools)
		defer destroy(sink)
		defer destroy(stage2)
		defer destroy(stage1)

		// Send 32 messages through a 4-slot pool (requires 8x full pool recycling!)
		num_msgs :: 32
		sched.pending_tokens = num_msgs
		scheduler_start(&sched)

		for i := 0; i < num_msgs; i += 1 {
			send(&sched, &pools[0], stage1, 5)
		}

		scheduler_wait_tokens(&sched)
		scheduler_wait_idle(&sched)

		// Each message starts at 5, adds +10 at Stage 1, +10 at Stage 2 -> arrives at Sink as 25
		testing.expect_value(t, stage1.state.count, num_msgs)
		testing.expect_value(t, stage2.state.count, num_msgs)
		testing.expect_value(t, sink.state.count, num_msgs)
		testing.expect_value(t, sink.state.sum, i64(num_msgs * 25))
	})
}
