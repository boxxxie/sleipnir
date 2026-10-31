package sleipnir_tests

import "base:intrinsics"
import "core:mem"
import "core:testing"
import "../actor"

// =============================================================================
// Test Utilities
// =============================================================================

// Runs a test procedure inside a `mem.Tracking_Allocator` and asserts that
// zero bytes were leaked and zero invalid frees occurred.
with_leak_check :: proc(t: ^testing.T, body: proc(t: ^testing.T)) {
	track: mem.Tracking_Allocator
	mem.tracking_allocator_init(&track, context.allocator)
	defer mem.tracking_allocator_destroy(&track)
	defer free_all(context.temp_allocator)

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
		q: actor.MPSC_Queue(Test_Node)
		actor.init(&q)
		testing.expect(t, actor.mpsc_is_empty(&q))

		n1 := Test_Node{val = 10}
		n2 := Test_Node{val = 20}
		n3 := Test_Node{val = 30}

		actor.push(&q, &n1)
		actor.push(&q, &n2)
		actor.push(&q, &n3)
		testing.expect(t, !actor.mpsc_is_empty(&q))

		testing.expect_value(t, actor.mpsc_pop_detached(&q).val, 10)
		testing.expect_value(t, actor.mpsc_pop_detached(&q).val, 20)
		testing.expect_value(t, actor.mpsc_pop_detached(&q).val, 30)

		testing.expect(t, actor.mpsc_is_empty(&q))
		testing.expect(t, actor.mpsc_pop_detached(&q) == nil)
	})
}

@(test)
test_02_mpsc_batch_chain_splice :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		q: actor.MPSC_Queue(Test_Node)
		actor.init(&q)

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
		actor.mpsc_push_chain(&q, &chain[0], &chain[3])

		for expected := 1; expected <= 4; expected += 1 {
			popped := actor.mpsc_pop_detached(&q)
			testing.expect(t, popped != nil)
			testing.expect_value(t, popped.val, expected)
		}
		testing.expect(t, actor.mpsc_is_empty(&q))
	})
}

@(test)
test_03_mpsc_dummy_tail_pop :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		q: actor.MPSC_Queue(Test_Node)
		actor.init(&q)
		q.stub.val = -1

		n1 := Test_Node{val = 100}
		n2 := Test_Node{val = 200}
		actor.push(&q, &n1)
		actor.push(&q, &n2)

		// Pop 1: returns `n1` as the message and retires the initial `stub` (-1)
		msg1, retired1, ok1 := actor.mpsc_pop_dummy(&q)
		testing.expect(t, ok1)
		testing.expect_value(t, msg1.val, 100)
		testing.expect_value(t, retired1.val, -1)

		// Pop 2: returns `n2` as the message and retires `n1` (100)
		msg2, retired2, ok2 := actor.mpsc_pop_dummy(&q)
		testing.expect(t, ok2)
		testing.expect_value(t, msg2.val, 200)
		testing.expect_value(t, retired2.val, 100)

		// Queue is now empty (`n2` remains as the new dummy tail sentinel)
		_, _, ok3 := actor.mpsc_pop_dummy(&q)
		testing.expect(t, !ok3)
	})
}

@(test)
test_04_runq_shard_stealing :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		shard: actor.RunQ_Shard(Test_Node)
		actor.init(&shard)

		// Empty shard returns nil on fast lock-free path
		testing.expect(t, actor.shard_pop_or_steal(&shard) == nil)

		n1 := Test_Node{val = 7}
		n2 := Test_Node{val = 8}
		actor.push(&shard, &n1)
		actor.push(&shard, &n2)

		testing.expect_value(t, actor.shard_pop_or_steal(&shard).val, 7)
		testing.expect_value(t, actor.shard_pop_or_steal(&shard).val, 8)
		testing.expect(t, actor.shard_pop_or_steal(&shard) == nil)
	})
}

// =============================================================================
// 2. Wait-Free SPSC Lane & Sharded SPSC Mailbox Tests (`spsc.odin`)
// =============================================================================

@(test)
test_05_spsc_lane_ring_wraparound :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		lane := new(actor.SPSC_Lane(int, 4))
		defer free(lane)

		// 1. Fill the 4-slot ring buffer to capacity
		testing.expect(t, actor.spsc_try_push(lane, 10))
		testing.expect(t, actor.spsc_try_push(lane, 20))
		testing.expect(t, actor.spsc_try_push(lane, 30))
		testing.expect(t, actor.spsc_try_push(lane, 40))
		testing.expect(t, !actor.spsc_try_push(lane, 999)) // Full!

		// 2. Pop 2 items (`10` and `20`) to free 2 slots
		v1, ok1 := actor.spsc_try_pop(lane)
		v2, ok2 := actor.spsc_try_pop(lane)
		testing.expect(t, ok1 && ok2)
		testing.expect_value(t, v1, 10)
		testing.expect_value(t, v2, 20)

		// 3. Push 2 more items (`50` and `60`) across the ring wrap-around boundary
		actor.spsc_push_spin(lane, 50)
		actor.spsc_push_spin(lane, 60)

		// 4. Consume the remaining 4 items (`30, 40, 50, 60`) in a single batch
		sum := 0
		count := actor.spsc_consume_batch(lane, 10, &sum, proc(total: ^int, item: ^int) {
			total^ += item^
		})
		testing.expect_value(t, count, 4)
		testing.expect_value(t, sum, 30 + 40 + 50 + 60)

		// 5. Lane is now empty
		_, ok_empty := actor.spsc_try_pop(lane)
		testing.expect(t, !ok_empty)
		testing.expect_value(t, actor.spsc_consume_batch(lane, 10, &sum, proc(total: ^int, item: ^int) {}), 0)
	})
}

@(test)
test_06_sharded_spsc_mailbox :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		mbox: actor.Sharded_SPSC_Mailbox(int, 16)
		actor.init(&mbox, num_producers = 3)
		defer actor.destroy(&mbox)

		// Producer 0 pushes 1, Producer 1 pushes 10, Producer 2 pushes 100
		actor.push(&mbox, 0, 1)
		actor.push(&mbox, 0, 1)
		actor.push(&mbox, 1, 10)
		actor.push(&mbox, 2, 100)

		total := 0
		consumed := actor.sharded_spsc_consume(&mbox, 8, &total, proc(acc: ^int, msg: ^int) {
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
		pools := make([]actor.Envelope_Pool(int), 2)
		actor.init(&pools[0], pool_id = 0, slab_capacity = 4)
		actor.init(&pools[1], pool_id = 1, slab_capacity = 4)
		defer {
			actor.destroy(&pools[0])
			actor.destroy(&pools[1])
			delete(pools)
		}

		// 1. Exhaust all 4 slab slots in Pool 0 & Pool 1 and collect them
		collector: actor.Batch_Return_Collector(int)
		for i := 0; i < 4; i += 1 {
			e0 := actor.pool_acquire(&pools[0])
			e1 := actor.pool_acquire(&pools[1])
			actor.collector_add(&collector, e0)
			actor.collector_add(&collector, e1)
		}

		// 2. Test heap fallback when `spin_if_exhausted = false` and slab is empty
		heap_env := actor.pool_acquire(&pools[0], spin_if_exhausted = false)
		testing.expect(t, (heap_env.flags & actor.ENVELOPE_FLAG_HEAP) != 0)
		free(heap_env, pools[0].allocator)

		// 3. Flush Pool 0 as local (`current_wid = 0`) and Pool 1 as remote (`remote_returns`)
		actor.collector_flush(&collector, pools, current_wid = 0)
		testing.expect(t, pools[0].local_free != nil)

		// 4. Re-acquire 4 envelopes from Pool 0 (from `local_free`) and Pool 1 (from `remote_returns`)
		batch_vals := [4]int{10, 20, 30, 40}
		first0, last0 := actor.pool_acquire_chain(&pools[0], batch_vals[:])
		first1, last1 := actor.pool_acquire_chain(&pools[1], batch_vals[:])
		testing.expect_value(t, first0.msg, 10)
		testing.expect_value(t, last0.msg, 40)
		testing.expect_value(t, first1.msg, 10)
		testing.expect_value(t, last1.msg, 40)
	})
}

@(test)
test_08_watermark_slab_fifo_and_chain :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		wm: actor.Producer_Watermark
		slab: actor.Watermark_Slab(int, 8)
		actor.init(&slab, prod_id = 0)
		defer actor.destroy(&slab)

		// Acquire 6 slots (out of 8) one-by-one
		for i := 0; i < 6; i += 1 {
			slot := actor.watermark_slab_acquire(&slab, &wm)
			slot.msg = i
		}

		// Advance the consumer watermark by 6 so the 8-slot FIFO ring can wrap around
		intrinsics.atomic_add_explicit(&wm.retired_count, 6, .Release)

		// Acquire a pre-linked chain of 4 slots across the wrap-around boundary
		first, last := actor.watermark_slab_acquire_chain(&slab, &wm, 4, 99)
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
counter_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Counter_State, i64), msg: ^i64) {
	self.state.total += msg^
	self.state.messages += 1
}

@(test)
test_09_actor_basic_counter :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sched: actor.Scheduler
		actor.init(&sched, actor.Scheduler_Config{num_workers = 2})
		defer actor.destroy(&sched)

		pools := make([]actor.Envelope_Pool(i64), 1)
		actor.init(&pools[0], 0, 32)
		defer {
			actor.destroy(&pools[0])
			delete(pools)
		}

		counter := actor.spawn(Counter_State{}, counter_receive, pools)
		defer actor.destroy(counter)

		actor.scheduler_start(&sched)

		// Send numbers 1..10 to `counter`
		for i: i64 = 1; i <= 10; i += 1 {
			actor.send(&sched, &pools[0], counter, i)
		}

		actor.scheduler_wait_messages(&sched, 10)
		actor.scheduler_wait_idle(&sched)

		testing.expect_value(t, counter.state.messages, 10)
		testing.expect_value(t, counter.state.total, 55)
	})
}

@(test)
test_10_actor_batch_chain_send :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sched: actor.Scheduler
		actor.init(&sched, actor.Scheduler_Config{num_workers = 1})
		defer actor.destroy(&sched)

		pools := make([]actor.Envelope_Pool(i64), 1)
		actor.init(&pools[0], 0, 32)
		defer {
			actor.destroy(&pools[0])
			delete(pools)
		}

		counter := actor.spawn(Counter_State{}, counter_receive, pools)
		defer actor.destroy(counter)

		actor.scheduler_start(&sched)

		// Send 4 batches of 5 messages each (20 messages total) via `send_batch_from_pool`
		batch := [5]i64{2, 4, 6, 8, 10} // Sum = 30 per batch
		for b := 0; b < 4; b += 1 {
			actor.send(&sched, &pools[0], counter, batch[:])
		}

		actor.scheduler_wait_messages(&sched, 20)
		actor.scheduler_wait_idle(&sched)

		testing.expect_value(t, counter.state.messages, 20)
		testing.expect_value(t, counter.state.total, 120)
	})
}

@(private = "file")
Double_Request :: struct {
	number:   i64,
	reply_to: ^actor.Actor(Counter_State, i64),
}

@(private = "file")
Doubler_State :: struct {
	requests_handled: int,
}

@(test)
test_11_actor_heterogeneous_pipeline_with_futex_parking :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sched: actor.Scheduler
		actor.init(&sched, actor.Scheduler_Config{
			num_workers    = 2,
			enable_parking = true, // Exercise adaptive 2-phase Futex sleep/wake
		})
		defer actor.destroy(&sched)

		req_pools   := make([]actor.Envelope_Pool(Double_Request), 2)
		reply_pools := make([]actor.Envelope_Pool(i64), 2)
		for w := 0; w < 2; w += 1 {
			actor.init(&req_pools[w], u16(w), 32)
			actor.init(&reply_pools[w], u16(w), 32)
		}
		defer {
			for w := 0; w < 2; w += 1 {
				actor.destroy(&req_pools[w])
				actor.destroy(&reply_pools[w])
			}
			delete(req_pools)
			delete(reply_pools)
		}

		// Actor 1: Doubles incoming numbers and sends the result to `msg.reply_to`
		doubler := actor.spawn(
			Doubler_State{},
			proc(ctx: ^actor.Context, self: ^actor.Actor(Doubler_State, Double_Request), msg: ^Double_Request) {
				self.state.requests_handled += 1
				actor.send(ctx, msg.reply_to, msg.number * 2)
			},
			req_pools,
		)
		defer actor.destroy(doubler)

		// Actor 2: Sums the doubled replies and decrements `sched.pending_tokens`
		aggregator := actor.spawn(
			Counter_State{},
			proc(ctx: ^actor.Context, self: ^actor.Actor(Counter_State, i64), msg: ^i64) {
				self.state.total += msg^
				self.state.messages += 1
				actor.complete_token(ctx, 1)
			},
			reply_pools,
		)
		defer actor.destroy(aggregator)

		sched.pending_tokens = 20
		actor.scheduler_start(&sched)

		for i: i64 = 1; i <= 20; i += 1 {
			actor.send(&sched, &req_pools[0], doubler, Double_Request{
				number   = i,
				reply_to = aggregator,
			})
		}

		actor.scheduler_wait_tokens(&sched)
		actor.scheduler_wait_idle(&sched)

		testing.expect_value(t, doubler.state.requests_handled, 20)
		testing.expect_value(t, aggregator.state.messages, 20)
		// Sum of 2 * (1..20) = 20 * 21 = 420
		testing.expect_value(t, aggregator.state.total, 420)
	})
}

@(private = "file")
Stage_State :: struct {
	next_stage: ^actor.Actor(Stage_State, i64),
	sum:        i64,
	count:      int,
}

@(test)
test_12_actor_zero_copy_forward_returns_to_pool :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sched: actor.Scheduler
		actor.init(&sched, actor.Scheduler_Config{
			num_workers = 2,
			batch_quota = 4,
		})
		defer actor.destroy(&sched)

		// Allocate a tiny 4-slot pool to prove that envelopes forwarded through
		// Stage 1 -> Stage 2 -> Sink are properly recycled back to the pool by Sink!
		pools := make([]actor.Envelope_Pool(i64), 1)
		actor.init(&pools[0], 0, slab_capacity = 4)
		defer {
			actor.destroy(&pools[0])
			delete(pools)
		}

		stage_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Stage_State, i64), msg: ^i64) {
			self.state.count += 1
			if self.state.next_stage != nil {
				// Forward the exact same Envelope(i64) in-place (`iso`) with +10 added
				actor.forward(ctx, self.state.next_stage, msg^ + 10)
			} else {
				// Final sink stage: does NOT forward, so `step_proc` recycles the envelope!
				self.state.sum += msg^
				actor.complete_token(ctx, 1)
			}
		}

		sink   := actor.spawn(Stage_State{next_stage = nil},    stage_receive, pools)
		stage2 := actor.spawn(Stage_State{next_stage = sink},   stage_receive, pools)
		stage1 := actor.spawn(Stage_State{next_stage = stage2}, stage_receive, pools)
		defer actor.destroy(sink)
		defer actor.destroy(stage2)
		defer actor.destroy(stage1)

		// Send 32 messages through a 4-slot pool (requires 8x full pool recycling!)
		num_msgs :: 32
		sched.pending_tokens = num_msgs
		actor.scheduler_start(&sched)

		for i := 0; i < num_msgs; i += 1 {
			actor.send(&sched, &pools[0], stage1, 5)
		}

		actor.scheduler_wait_tokens(&sched)
		actor.scheduler_wait_idle(&sched)

		// Each message starts at 5, adds +10 at Stage 1, +10 at Stage 2 -> arrives at Sink as 25
		testing.expect_value(t, stage1.state.count, num_msgs)
		testing.expect_value(t, stage2.state.count, num_msgs)
		testing.expect_value(t, sink.state.count, num_msgs)
		testing.expect_value(t, sink.state.sum, i64(num_msgs * 25))
	})
}

// =============================================================================
// 5. Ergonomic DSL Features: `scoped_scheduler`, Ambient Context, Auto-Pools, & `PID`
// =============================================================================

@(test)
test_13_scoped_scheduler_and_auto_pooled_ambient_api :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		final_total: i64 = 0
		final_msgs:  int = 0
		{
			// 1-line RAII scheduler scope (`@(deferred_out = scheduler_scope_end)`)
			_ = actor.scoped_scheduler(num_workers = 2)

			// Spawn actor with 2-arg `(state: ^State, msg: ^Msg)` handler — no manual pool or defer destroy!
			counter := actor.spawn(Counter_State{}, proc(state: ^Counter_State, msg: ^i64) {
				state.total += msg^
				state.messages += 1
			})

			// Ambient single-message sends (`actor.send(target, msg)`)
			for i: i64 = 1; i <= 10; i += 1 {
				actor.send(counter, i)
			}

			// Ambient batch slice splice (`actor.send(target, batch[:])`)
			batch := [4]i64{10, 20, 30, 40}
			actor.send(counter, batch[:])

			actor.wait_messages(14)
			actor.wait_idle()

			final_total = counter.state.total
			final_msgs  = counter.state.messages
		}
		// At block exit, `scoped_scheduler` automatically destroyed `counter`, its auto-pools, and the scheduler!
		testing.expect_value(t, final_msgs, 14)
		testing.expect_value(t, final_total, 55 + 100)
	})
}

@(private = "file")
Calc_Add :: struct {
	amount: i64,
}
@(private = "file")
Calc_Mul :: struct {
	factor: i64,
}
@(private = "file")
Calc_Report :: struct {
	reply_to: actor.PID(i64), // Decoupled from the sink actor's private `$State`!
}
@(private = "file")
Calc_Msg :: union {
	Calc_Add,
	Calc_Mul,
	Calc_Report,
}

@(test)
test_14_pid_handle_and_tagged_union_value_behaviors :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		reported_val: i64 = 0
		{
			sched := actor.scoped_scheduler(num_workers = 2)
			sched.pending_tokens = 1

			// Sink actor accepts `i64`
			sink := actor.spawn(Counter_State{}, proc(state: ^Counter_State, val: i64) {
				state.total = val
				actor.complete_token() // Ambient 0-arg `complete_token()`!
			})

			// Calculator actor with multi-behavior tagged `union` (`Calc_Msg` by value)
			calc := actor.spawn(Counter_State{}, proc(state: ^Counter_State, msg: Calc_Msg) {
				switch m in msg {
				case Calc_Add:
					state.total += m.amount
				case Calc_Mul:
					state.total *= m.factor
				case Calc_Report:
					actor.send(m.reply_to, state.total) // Ambient send to `PID(i64)`!
				}
			})

			calc_pid := actor.pid(calc)
			actor.send(calc_pid, Calc_Msg(Calc_Add{amount = 7}))
			actor.send(calc_pid, Calc_Msg(Calc_Mul{factor = 6}))
			actor.send(calc_pid, Calc_Msg(Calc_Report{reply_to = actor.pid(sink)}))

			actor.wait_tokens()
			actor.wait_idle()
			reported_val = sink.state.total
		}
		testing.expect_value(t, reported_val, 42)
	})
}

@(private = "file")
Hop_Stage :: struct {
	next_pid: actor.PID(i64),
	sum:      i64,
}

@(test)
test_15_ambient_zero_copy_forward_and_unchanged_forward :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		final_sum: i64 = 0
		{
			sched := actor.scoped_scheduler(num_workers = 2)
			sched.pending_tokens = 10

			on_hop :: proc(state: ^Hop_Stage, msg: ^i64) {
				if state.next_pid.ref != nil {
					// Mutate `msg^` in place and forward the envelope via 1-arg `forward(next_pid)`
					msg^ += 5
					actor.forward(state.next_pid)
				} else {
					state.sum += msg^
					actor.complete_token()
				}
			}

			sink   := actor.spawn(Hop_Stage{}, on_hop)
			stage2 := actor.spawn(Hop_Stage{next_pid = actor.pid(sink)}, on_hop)
			stage1 := actor.spawn(Hop_Stage{next_pid = actor.pid(stage2)}, on_hop)

			for i := 0; i < 10; i += 1 {
				actor.send(stage1, i64(10))
			}

			actor.wait_tokens()
			actor.wait_idle()
			final_sum = sink.state.sum
		}
		// 10 messages * (10 + 5 + 5) = 200
		testing.expect_value(t, final_sum, 200)
	})
}

@(private = "file")
Self_Contained_Actor :: struct {
	accum:   i64,
	receive: proc(self: ^Self_Contained_Actor, msg: i64),
}

@(test)
test_16_compile_time_structural_typing_spawn_self :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		result: i64 = 0
		{
			_ = actor.scoped_scheduler(num_workers = 2)

			// Spawn a struct that embeds its own `receive` behavior using `base:intrinsics`!
			a := actor.spawn_self(Self_Contained_Actor{
				accum = 100,
				receive = proc(self: ^Self_Contained_Actor, msg: i64) {
					self.accum += msg
				},
			})

			actor.send(a, i64(23))
			actor.send(a, i64(19))

			actor.wait_messages(2)
			actor.wait_idle()
			result = a.state.accum
		}
		testing.expect_value(t, result, 142)
	})
}

// ============================================================================
// TEST 17: Synchronous `actor.call`, `actor.reply`, and Envelope Reply Routing
// ============================================================================

@(private = "file")
Ledger_Add       :: struct { x: i64 }
@(private = "file")
Ledger_Get_Total :: struct {}

@(private = "file")
Ledger_Msg :: union {
	Ledger_Add,
	Ledger_Get_Total,
}

@(private = "file")
Ledger_State :: struct {
	total: i64,
}

@(test)
test_17_actor_call_and_envelope_reply :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sync_total:  i64 = 0
		async_total: i64 = 0
		{
			// Use a 1-worker scheduler so we also verify cooperative worker-thread stepping!
			_ = actor.scoped_scheduler(num_workers = 1)

			ledger := actor.spawn(Ledger_State{total = 10}, proc(s: ^Ledger_State, m: Ledger_Msg) {
				switch v in m {
				case Ledger_Add:
					s.total += v.x
				case Ledger_Get_Total:
					// Reply address lives in the Envelope header, NOT inside `Ledger_Get_Total`!
					actor.reply(s.total)
				}
			})

			actor.send(ledger, Ledger_Msg(Ledger_Add{x = 25}))
			actor.send(ledger, Ledger_Msg(Ledger_Add{x = 7}))

			// 1. Synchronous `call` from main thread
			sync_total = actor.call(ledger, Ledger_Msg(Ledger_Get_Total{}), i64)

			// 2. Asynchronous `send(target, msg, reply_to)` forwarding reply to another actor
			receiver := actor.spawn(i64(0), proc(state: ^i64, val: i64) {
				state^ = val
			})
			actor.send(ledger, Ledger_Msg(Ledger_Get_Total{}), actor.pid(receiver))

			actor.wait_messages(5)
			actor.wait_idle()
			async_total = receiver.state
		}

		testing.expect_value(t, sync_total, 42)
		testing.expect_value(t, async_total, 42)
	})
}

// ============================================================================
// Layer 1: Low-Level Runtime Specifications (Tests 18 & 19)
// ============================================================================

@(test)
test_18_actor_to_actor_cooperative_call_and_reply_overloads :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		// Single worker thread (`num_workers = 1`): nested `call` inside a handler
		// on Worker 0 must cooperatively step `math_worker` without deadlocking.
		rt := runtime_scenario(t, actor.scoped_scheduler(num_workers = 1))

		math_worker  := given_square_reply_worker(&rt)
		orchestrator := given_square_orchestrator(&rt)
		sink         := given_probe(&rt, i64)

		// 1. Cooperative actor-to-actor synchronous `call`: (9 * 9) + 10 = 91 (3 messages)
		upon_orchestrating_square(orchestrator,
			worker = actor.pid(math_worker),
			sink   = sink,
			input  = 9,
			addend = 10,
		)
		then_actor_state_is(&rt, sink, 91, messages = 3)

		// 2. Async `send(..., reply_to)` overloads for both Actor handle and PID (2 messages each):
		then_async_reply_resolves(&rt, math_worker,
			req      = Compute_Square{x = 7},
			expected = 49,
			via      = .Actor_Handle,
		)
		then_async_reply_resolves(&rt, math_worker,
			req      = Compute_Square{x = 11},
			expected = 121,
			via      = .PID,
		)

		// Verify `math_worker` state incremented across all 3 requests:
		then_actor_state_is(&rt, math_worker, 3)
	})
}

@(test)
test_19_batch_send_pid_and_early_actor_destroy :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		rt := runtime_scenario(t, actor.scoped_scheduler(num_workers = 2))

		acc       := given_accumulator(initial = i64(0))
		forwarder := given_batch_and_value_forwarder(
			target     = actor.pid(acc),
			batch      = {100, 200},
			multiplier = 10,
		)

		// 1. Ambient batch send to `PID(i64)` (`send_ambient_batch_pid`): 10 + 20 + 30 = 60 (3 messages)
		upon_sending_batch_to_pid(actor.pid(acc), 10, 20, 30)
		then_actor_state_is(&rt, acc, 60, messages = 3)

		// 2. Worker-context batch send to `PID(i64)` (`send_ctx_batch_pid`) +
		//    2-arg ambient `forward(pid, updated_msg)`: 60 + 100 + 200 + (5 * 10) = 410 (4 messages)
		upon_sending(forwarder, i64(5))
		then_actor_state_is(&rt, acc, 410, messages = 4)

		// 3. Explicit early destruction before `scoped_scheduler` exits must
		//    unregister from `s.owned_actors` without double-free on scope exit:
		upon_destroying_early(forwarder)
	})
}

// ============================================================================
// Layer 2: Scenario-Specific Fixtures (Tests 18 & 19)
// ============================================================================

@(private = "file")
Compute_Square :: struct {
	x: i64,
}

@(private = "file")
Orchestrator_Run :: struct {
	worker_pid: actor.PID(Compute_Square),
	sink_actor: ^actor.Actor(i64, i64),
	input:      i64,
	addend:     i64,
}

@(private = "file")
Forwarder_State :: struct {
	target_pid: actor.PID(i64),
	batch:      [2]i64,
	multiplier: i64,
}

// Exercises 3-arg `(ctx: ^Context, state: ^State, msg: ^Msg)` overload (`spawn_sched_state_auto`):
@(private = "file")
given_square_reply_worker :: proc(rt: ^Runtime_Spec) -> ^actor.Actor(i64, Compute_Square) {
	return actor.spawn(rt.sched, i64(0), proc(ctx: ^actor.Context, calls: ^i64, req: ^Compute_Square) {
		calls^ += 1
		actor.reply(ctx, req.x * req.x)
	})
}

@(private = "file")
given_square_orchestrator :: proc(rt: ^Runtime_Spec) -> ^actor.Actor(i64, Orchestrator_Run) {
	return actor.spawn(rt.sched, i64(0), proc(state: ^i64, cmd: Orchestrator_Run) {
		sq := actor.call(cmd.worker_pid, Compute_Square{x = cmd.input}, i64)
		state^ = sq + cmd.addend
		actor.send(cmd.sink_actor, state^)
	})
}

// Preserves `i64` as the incoming message type so `forward(s.target_pid, ...)`
// exercises the exact same `i64 -> PID(i64)` envelope forwarding path:
@(private = "file")
given_batch_and_value_forwarder :: proc(
	target:     actor.PID(i64),
	batch:      [2]i64,
	multiplier: i64,
) -> ^actor.Actor(Forwarder_State, i64) {
	init_state := Forwarder_State{
		target_pid = target,
		batch      = batch,
		multiplier = multiplier,
	}
	return actor.spawn(init_state, proc(s: ^Forwarder_State, x: ^i64) {
		b := s.batch
		actor.send(s.target_pid, b[:])
		actor.forward(s.target_pid, x^ * s.multiplier)
	})
}

@(private = "file")
upon_orchestrating_square :: proc(
	orchestrator: ^actor.Actor(i64, Orchestrator_Run),
	worker:       actor.PID(Compute_Square),
	sink:         ^actor.Actor(i64, i64),
	input:        i64,
	addend:       i64,
) {
	actor.send(orchestrator, Orchestrator_Run{
		worker_pid = worker,
		sink_actor = sink,
		input      = input,
		addend     = addend,
	})
}

// ============================================================================
// Layer 3: Reusable Suite-Wide Concurrency Protocol Driver
// ============================================================================

@(private = "file")
Addressing_Mode :: enum {
	Actor_Handle,
	PID,
}

@(private = "file")
Runtime_Spec :: struct {
	t:          ^testing.T,
	sched:      ^actor.Scheduler,
	total_msgs: i64,
}

@(private = "file")
runtime_scenario :: proc(t: ^testing.T, sched: ^actor.Scheduler) -> Runtime_Spec {
	return Runtime_Spec{t = t, sched = sched}
}

// Reusable test probe actor that records the latest message of type `T`:
@(private = "file")
given_probe :: proc(rt: ^Runtime_Spec, $T: typeid) -> ^actor.Actor(T, T) {
	return actor.spawn(rt.sched, T{}, proc(state: ^T, val: T) {
		state^ = val
	})
}

// Exercises ambient scheduler `spawn(initial_state, handler)` overload:
@(private = "file")
given_accumulator :: proc(initial: $T) -> ^actor.Actor(T, T) {
	return actor.spawn(initial, proc(total: ^T, x: T) {
		total^ += x
	})
}

@(private = "file")
upon_sending_batch_to_pid :: proc(target_pid: actor.PID($T), items: ..T) {
	actor.send(target_pid, items)
}

@(private = "file")
upon_sending :: proc(target: ^actor.Actor($S, $M), msg: M) {
	actor.send(target, msg)
}

@(private = "file")
upon_destroying_early :: proc(target: ^actor.Actor($S, $M)) {
	actor.destroy(target)
}

@(private = "file")
then_async_reply_resolves :: proc(
	rt:       ^Runtime_Spec,
	worker:   ^actor.Actor($S, $Req),
	req:      Req,
	expected: $Resp,
	via:      Addressing_Mode,
	messages: i64 = 2,
) {
	probe := given_probe(rt, Resp)
	switch via {
	case .Actor_Handle:
		actor.send(worker, req, probe)
	case .PID:
		actor.send(actor.pid(worker), req, probe)
	}
	then_actor_state_is(rt, probe, expected, messages = messages)
}

@(private = "file")
then_actor_state_is :: proc(
	rt:       ^Runtime_Spec,
	target:   ^actor.Actor($S, $M),
	expected: S,
	messages: i64 = 0,
) {
	if messages > 0 {
		rt.total_msgs += messages
		actor.wait_messages(rt.total_msgs)
	}
	actor.wait_idle()
	testing.expect_value(rt.t, target.state, expected)
}
