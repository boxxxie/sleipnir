package sleipnir_tests

import "base:intrinsics"
import "base:runtime"
import "core:c/libc"
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

// =============================================================================
// 6. Runtime Concurrency & Safety Hardening Tests (Track 1)
// =============================================================================

@(test)
test_20_mpsc_spsc_and_pool_hardening_invariants :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		// 1. `spsc_consume_batch` must return 0 cleanly when `max_batch <= 0` without u64 wraparound
		lane := new(actor.SPSC_Lane(int, 8))
		defer free(lane)
		testing.expect(t, actor.spsc_try_push(lane, 11))
		testing.expect(t, actor.spsc_try_push(lane, 22))
		sum := 0
		handler := proc(acc: ^int, item: ^int) { acc^ += item^ }
		testing.expect_value(t, actor.spsc_consume_batch(lane, 0, &sum, handler), 0)
		testing.expect_value(t, actor.spsc_consume_batch(lane, -5, &sum, handler), 0)
		testing.expect_value(t, sum, 0)
		testing.expect_value(t, actor.spsc_consume_batch(lane, 8, &sum, handler), 2)
		testing.expect_value(t, sum, 33)

		// 2. `mpsc_init` must initialize `stub.pool_id = POOL_ID_NONE` (0xFFFF) and `ENVELOPE_FLAG_STATIC`
		q: actor.MPSC_Queue(actor.Envelope(int))
		actor.mpsc_init(&q)
		testing.expect_value(t, q.stub.pool_id, actor.POOL_ID_NONE)
		testing.expect(t, (q.stub.flags & actor.ENVELOPE_FLAG_STATIC) != 0)

		slab: actor.Watermark_Slab(int, 8)
		wm0: actor.Producer_Watermark
		actor.init(&slab, prod_id = 0)
		defer actor.destroy(&slab)

		wm_env := actor.watermark_slab_acquire(&slab, &wm0)
		wm_env.msg = 77
		actor.mpsc_push(&q, wm_env)
		popped_msg, retired_stub, pop_ok := actor.mpsc_pop_dummy(&q)
		testing.expect(t, pop_ok)
		testing.expect_value(t, popped_msg.msg, 77)
		testing.expect_value(t, retired_stub.pool_id, actor.POOL_ID_NONE)

		// 3. Recycled envelopes from `pool_acquire`, `watermark_slab_acquire`, and
		//    `watermark_slab_acquire_chain` must clear `reply_slot` and `flags`
		pools := make([]actor.Envelope_Pool(int), 2)
		actor.init(&pools[0], pool_id = 0, slab_capacity = 2)
		actor.init(&pools[1], pool_id = 1, slab_capacity = 2)
		defer {
			actor.destroy(&pools[0])
			actor.destroy(&pools[1])
			delete(pools)
		}

		e_local := actor.pool_acquire(&pools[0])
		e_local.reply_slot = rawptr(uintptr(0xDEADBEEF))
		e_local.flags = actor.ENVELOPE_FLAG_CALL

		e_remote := actor.pool_acquire(&pools[1])
		e_remote.reply_slot = rawptr(uintptr(0xCAFEBABE))
		e_remote.flags = actor.ENVELOPE_FLAG_REPLY_PID

		collector: actor.Batch_Return_Collector(int)
		actor.collector_add(&collector, e_local)
		actor.collector_add(&collector, e_remote)
		actor.collector_flush(&collector, pools, current_wid = 0)

		// Re-acquire from `local_free` (pool 0) and `remote_returns` (pool 1)
		re_local := actor.pool_acquire(&pools[0])
		testing.expect(t, re_local.reply_slot == nil)
		testing.expect_value(t, re_local.flags, actor.ENVELOPE_FLAG_NONE)

		re_remote := actor.pool_acquire(&pools[1])
		testing.expect(t, re_remote.reply_slot == nil)
		testing.expect_value(t, re_remote.flags, actor.ENVELOPE_FLAG_NONE)

		// Re-acquire after dirtying `Watermark_Slab` slots across ring wraparound
		wm: actor.Producer_Watermark
		wm_slab: actor.Watermark_Slab(int, 8)
		actor.init(&wm_slab, prod_id = 0)
		defer actor.destroy(&wm_slab)

		for i := 0; i < 6; i += 1 {
			s_env := actor.watermark_slab_acquire(&wm_slab, &wm)
			s_env.reply_slot = rawptr(uintptr(0xBEEF))
			s_env.flags = actor.ENVELOPE_FLAG_CALL
		}
		intrinsics.atomic_add_explicit(&wm.retired_count, 6, .Release)
		wrapped_single := actor.watermark_slab_acquire(&wm_slab, &wm)
		testing.expect(t, wrapped_single.reply_slot == nil)
		testing.expect_value(t, wrapped_single.flags, actor.ENVELOPE_FLAG_NONE)

		intrinsics.atomic_add_explicit(&wm.retired_count, 2, .Release)
		c_first, c_last := actor.watermark_slab_acquire_chain(&wm_slab, &wm, 4, 123)
		for cur := c_first; cur != nil; cur = cur.next {
			testing.expect(t, cur.reply_slot == nil)
			testing.expect_value(t, cur.flags, actor.ENVELOPE_FLAG_NONE)
			if cur == c_last { break }
		}

		// 4. `pool_init` with `slab_capacity <= 0` forces `heap_fallback` instead of deadlocking
		zero_pool: actor.Envelope_Pool(int)
		actor.init(&zero_pool, pool_id = 0, slab_capacity = 0, heap_fallback = false)
		defer actor.destroy(&zero_pool)
		testing.expect(t, zero_pool.heap_fallback)
		if zero_pool.heap_fallback {
			zero_env := actor.pool_acquire(&zero_pool)
			testing.expect(t, (zero_env.flags & actor.ENVELOPE_FLAG_HEAP) != 0)
			testing.expect(t, zero_env.reply_slot == nil)
			free(zero_env, zero_pool.allocator)
		}
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

// =============================================================================
// 6. Phase 2 Runtime Hardening Tests (Tests 21 - 23)
// =============================================================================

@(private = "file")
test21_custom_logger_proc :: proc(
	data:     rawptr,
	level:    runtime.Logger_Level,
	text:     string,
	options:  runtime.Logger_Options,
	location := #caller_location,
) {
}

@(private = "file")
Counting_Temp_State :: struct {
	backing:        runtime.Allocator,
	alloc_count:    int,
	free_all_count: int,
}

@(private = "file")
counting_temp_alloc_proc :: proc(
	allocator_data: rawptr,
	mode:           runtime.Allocator_Mode,
	size, alignment: int,
	old_memory:     rawptr,
	old_size:       int,
	location:       runtime.Source_Code_Location = #caller_location,
) -> ([]byte, runtime.Allocator_Error) {
	st := cast(^Counting_Temp_State)allocator_data
	#partial switch mode {
	case .Alloc, .Alloc_Non_Zeroed:
		intrinsics.atomic_add_explicit(&st.alloc_count, 1, .Relaxed)
	case .Free_All:
		intrinsics.atomic_add_explicit(&st.free_all_count, 1, .Release)
	}
	return st.backing.procedure(st.backing.data, mode, size, alignment, old_memory, old_size, location)
}

@(test)
test_21_worker_context_inheritance_and_temp_allocator_discipline :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		// 1. Non-worker thread with non-nil `context.user_ptr` must still get `current_context() == nil`.
		saved_user_ptr := context.user_ptr
		context.user_ptr = rawptr(uintptr(0xCAFEBABE))
		testing.expect(t, actor.current_context() == nil)
		context.user_ptr = saved_user_ptr

		// 2. Worker threads inherit caller's `logger`, `user_index`, and allocator (`Tracking_Allocator`).
		saved_logger := context.logger
		saved_user_index := context.user_index
		context.logger = runtime.Logger{procedure = test21_custom_logger_proc}
		context.user_index = 42

		inner_track: mem.Tracking_Allocator
		mem.tracking_allocator_init(&inner_track, context.allocator)
		defer mem.tracking_allocator_destroy(&inner_track)
		saved_alloc := context.allocator
		context.allocator = mem.tracking_allocator(&inner_track)

		sched: actor.Scheduler
		err := actor.init(&sched, actor.Scheduler_Config{num_workers = 2, enable_parking = false})
		testing.expect_value(t, err, mem.Allocator_Error.None)

		Worker_Seen :: struct {
			logger_matched: bool,
			user_index:     int,
			ctx_non_nil:    bool,
			heap_val:       ^int,
			alloc_count:    int,
		}

		// Re-bind `context.allocator` back to `inner_track` on main too before `scheduler_start`.
		seen_actor := actor.spawn(&sched, Worker_Seen{}, proc(s: ^Worker_Seen, cmd: int) {
			if cmd == 1 {
				s.logger_matched = context.logger.procedure == test21_custom_logger_proc
				s.user_index = context.user_index
				s.ctx_non_nil = actor.current_context() != nil
				s.heap_val = new(int)
				s.heap_val^ = 777
			} else if cmd == 2 {
				free(s.heap_val)
				s.heap_val = nil
			}
		})

		actor.scheduler_start(&sched)
		before_worker_allocs := len(inner_track.allocation_map)
		actor.send(&sched, seen_actor, 1)
		actor.wait_idle(&sched)

		testing.expect(t, seen_actor.state.logger_matched)
		testing.expect_value(t, seen_actor.state.user_index, 42)
		testing.expect(t, seen_actor.state.ctx_non_nil)
		testing.expect(t, len(inner_track.allocation_map) > before_worker_allocs)

		actor.send(&sched, seen_actor, 2)
		actor.wait_idle(&sched)
		actor.destroy(&sched)

		context.allocator = saved_alloc
		context.logger = saved_logger
		context.user_index = saved_user_index
		testing.expect_value(t, len(inner_track.allocation_map), 0)
		testing.expect_value(t, len(inner_track.bad_free_array), 0)

		// 3. Top-level turn temp allocator wipe without wiping nested cooperative `actor.call` scratch.
		Temp_State :: struct {
			temp_ptr:       ^Counting_Temp_State,
			callee_pid:     actor.PID(i64),
			scratch_ok:     bool,
			free_all_same:  bool,
			call_result:    i64,
		}

		temp_arena_buf := make([]u8, 64 * 1024)
		defer delete(temp_arena_buf)
		temp_arena: mem.Arena
		mem.arena_init(&temp_arena, temp_arena_buf)

		count_state := Counting_Temp_State{
			backing = mem.arena_allocator(&temp_arena),
		}
		saved_temp := context.temp_allocator
		context.temp_allocator = runtime.Allocator{
			procedure = counting_temp_alloc_proc,
			data      = &count_state,
		}

		sched1: actor.Scheduler
		err1 := actor.init(&sched1, actor.Scheduler_Config{num_workers = 1, enable_parking = false})
		testing.expect_value(t, err1, mem.Allocator_Error.None)

		callee := actor.spawn(&sched1, i64(0), proc(ctx: ^actor.Context, st: ^i64, x: ^i64) {
			scratch := make([]u8, 32, context.temp_allocator)
			scratch[0] = 0x33
			st^ += i64(scratch[0])
			actor.reply(ctx, x^ * 3)
		})

		caller := actor.spawn(&sched1, Temp_State{temp_ptr = &count_state, callee_pid = actor.pid(callee)}, proc(s: ^Temp_State, cmd: i64) {
			if cmd == 1 {
				tmp := make([]u8, 64, context.temp_allocator)
				tmp[0] = 0x5A
				return
			}
			// Outer turn allocates scratch BEFORE cooperative `actor.call`.
			before := make([]u8, 64, context.temp_allocator)
			before[0] = 0xA5
			before[63] = 0xA5
			fa_before := intrinsics.atomic_load_explicit(&s.temp_ptr.free_all_count, .Acquire)

			s.call_result = actor.call(s.callee_pid, cmd, i64)

			fa_after_call := intrinsics.atomic_load_explicit(&s.temp_ptr.free_all_count, .Acquire)
			s.free_all_same = fa_after_call == fa_before
			s.scratch_ok = before[0] == 0xA5 && before[63] == 0xA5
		})

		actor.scheduler_start(&sched1)
		actor.send(&sched1, caller, i64(1))
		actor.wait_idle(&sched1)
		fa_after_msg1 := intrinsics.atomic_load_explicit(&count_state.free_all_count, .Acquire)
		testing.expect(t, fa_after_msg1 >= 1)

		actor.send(&sched1, caller, i64(14))
		actor.wait_idle(&sched1)
		actor.destroy(&sched1)
		context.temp_allocator = saved_temp

		testing.expect_value(t, caller.state.call_result, i64(42))
		testing.expect(t, caller.state.free_all_same)
		testing.expect(t, caller.state.scratch_ok)
		testing.expect(t, intrinsics.atomic_load_explicit(&count_state.free_all_count, .Acquire) > fa_after_msg1)
	})
}

@(thread_local)
_test22_jmp: ^libc.jmp_buf

@(thread_local)
_test22_caught: int

@(private = "file")
test22_trap_proc :: proc(prefix, message: string, loc: runtime.Source_Code_Location) -> ! {
	_test22_caught += 1
	if _test22_jmp != nil {
		libc.longjmp(_test22_jmp, 1)
	}
	runtime.default_assertion_failure_proc(prefix, message, loc)
}

@(test)
test_22_call_slot_offset_zero_uaf_stress_double_reply_and_cycle_guard :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		// 1. Offset-0 `ready: sync.Futex` invariant on `Call_Slot($Res)`.
		testing.expect_value(t, int(offset_of(actor.Call_Slot(i64), ready)), 0)

		// 2. Cross-thread `actor.call` / `actor.reply` stress with duplicate `reply` & `forward` delegation.
		sched: actor.Scheduler
		err := actor.init(&sched, actor.Scheduler_Config{num_workers = 4, enable_parking = true})
		testing.expect_value(t, err, mem.Allocator_Error.None)
		actor.scheduler_start(&sched)

		double_replier := actor.spawn(&sched, 0, proc(ctx: ^actor.Context, count: ^int, req: ^i64) {
			count^ += 1
			actor.reply(ctx, req^ * 2)
			// Duplicate reply must be a safe no-op (must NOT overwrite popped Call_Slot).
			actor.reply(ctx, req^ * 999)
		})

		Forward_Pair :: struct {
			next_pid: actor.PID(i64),
		}
		stage2 := actor.spawn(&sched, 0, proc(ctx: ^actor.Context, count: ^int, val: ^i64) {
			count^ += 1
			actor.reply(ctx, val^ + 1)
		})
		stage1 := actor.spawn(&sched, Forward_Pair{next_pid = actor.pid(stage2)}, proc(ctx: ^actor.Context, st: ^Forward_Pair, val: ^i64) {
			actor.forward(ctx, st.next_pid, val^ * 2)
			// Calling `reply` after `forward` must also be a safe no-op!
			actor.reply(ctx, i64(-1))
		})

		for i := 0; i < 1000; i += 1 {
			x := i64(i + 1)
			res := actor.call(&sched, double_replier, x, i64)
			testing.expect_value(t, res, x * 2)
		}
		fwd_res := actor.call(&sched, stage1, i64(21), i64)
		testing.expect_value(t, fwd_res, i64(43))
		actor.destroy(&sched)

		// 3. 1-worker self-call and `A -> B -> A` cycle detection must abort cleanly without hanging.
		sched1: actor.Scheduler
		err1 := actor.init(&sched1, actor.Scheduler_Config{num_workers = 1, enable_parking = false})
		testing.expect_value(t, err1, mem.Allocator_Error.None)

		Self_Caller_State :: struct {
			self_pid: actor.PID(i64),
			caught:   bool,
		}
		self_caller := actor.spawn(&sched1, Self_Caller_State{}, proc(st: ^Self_Caller_State, x: i64) {
			if x == 1 {
				prev_proc := context.assertion_failure_proc
				context.assertion_failure_proc = test22_trap_proc
				jb: libc.jmp_buf
				_test22_jmp = &jb
				_test22_caught = 0
				if libc.setjmp(&jb) == 0 {
					_ = actor.call(st.self_pid, i64(2), i64)
				} else {
					st.caught = _test22_caught > 0
				}
				_test22_jmp = nil
				context.assertion_failure_proc = prev_proc
			} else {
				actor.reply(x)
			}
		})
		self_caller.state.self_pid = actor.pid(self_caller)

		Mutual_State :: struct {
			peer_pid: actor.PID(i64),
			caught:   bool,
		}
		actor_a: ^actor.Actor(Mutual_State, i64)
		actor_b := actor.spawn(&sched1, Mutual_State{}, proc(ctx: ^actor.Context, st: ^Mutual_State, x: ^i64) {
			prev_proc := context.assertion_failure_proc
			context.assertion_failure_proc = test22_trap_proc
			jb: libc.jmp_buf
			_test22_jmp = &jb
			_test22_caught = 0
			if libc.setjmp(&jb) == 0 {
				_ = actor.call(st.peer_pid, x^ + 1, i64)
			} else {
				st.caught = _test22_caught > 0
			}
			_test22_jmp = nil
			context.assertion_failure_proc = prev_proc
			actor.reply(ctx, i64(-7))
		})
		actor_a = actor.spawn(&sched1, Mutual_State{peer_pid = actor.pid(actor_b)}, proc(st: ^Mutual_State, x: i64) {
			if x == 10 {
				res := actor.call(st.peer_pid, x, i64)
				st.caught = res == -7
			} else {
				actor.reply(x)
			}
		})
		actor_b.state.peer_pid = actor.pid(actor_a)

		actor.scheduler_start(&sched1)
		actor.send(&sched1, self_caller, i64(1))
		actor.wait_idle(&sched1)
		actor.send(&sched1, actor_a, i64(10))
		actor.wait_idle(&sched1)
		actor.destroy(&sched1)

		testing.expect(t, self_caller.state.caught)
		testing.expect(t, actor_b.state.caught)
		testing.expect(t, actor_a.state.caught)
	})
}

@(test)
test_23_dekker_wait_idle_heap_fallback_pool_id_and_cross_scheduler_send :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		// 1. Respect explicit `0 < cfg.num_shards < s.num_workers`.
		shard_sched: actor.Scheduler
		err_sh := actor.init(&shard_sched, actor.Scheduler_Config{num_workers = 4, num_shards = 1, enable_parking = false})
		testing.expect_value(t, err_sh, mem.Allocator_Error.None)
		testing.expect_value(t, shard_sched.num_shards, 1)
		actor.destroy(&shard_sched)

		// 2. Heap-fallback envelopes (`pool_id == POOL_ID_NONE`, `ENVELOPE_FLAG_HEAP`) do not corrupt `pools[0]` or leak on `actor.destroy`.
		tiny_pool := make([]actor.Envelope_Pool(int), 3)
		for i := 0; i < len(tiny_pool); i += 1 {
			actor.init(&tiny_pool[i], u16(i), slab_capacity = 2, heap_fallback = true)
		}
		defer {
			for i := 0; i < len(tiny_pool); i += 1 {
				actor.destroy(&tiny_pool[i])
			}
			delete(tiny_pool)
		}

		heap_sched: actor.Scheduler
		err_hs := actor.init(&heap_sched, actor.Scheduler_Config{num_workers = 2, enable_parking = true})
		testing.expect_value(t, err_hs, mem.Allocator_Error.None)
		defer actor.destroy(&heap_sched)

		// Spawn an actor BEFORE starting workers, overload its tiny 2-slot pool with 16 msgs -> forces 14 heap fallbacks,
		// then destroy the un-stepped actor cleanly (drains queued heap envelopes with 0 leaks).
		victim := actor.spawn(&heap_sched, 0, proc(ctx: ^actor.Context, self: ^actor.Actor(int, int), x: ^int) {
			self.state += x^
		}, pools = tiny_pool)
		for i := 0; i < 16; i += 1 {
			actor.send(&heap_sched, victim, i + 1)
		}
		// Drain on explicit destroy before starting scheduler, verify no leaks.
		actor.destroy(victim)

		// Verify normal execution with heap fallbacks also does not corrupt `pools[0]`.
		active := actor.spawn(&heap_sched, 0, proc(ctx: ^actor.Context, self: ^actor.Actor(int, int), x: ^int) {
			self.state += x^
		}, pools = tiny_pool)
		actor.scheduler_start(&heap_sched)
		for i := 0; i < 64; i += 1 {
			actor.send(&heap_sched, active, 1)
		}
		actor.wait_idle(&heap_sched)
		testing.expect_value(t, active.state, 64)

		// 3. Cross-scheduler `actor.send` and `actor.forward` must route to target's scheduler safely.
		s1: actor.Scheduler
		s2: actor.Scheduler
		err1 := actor.init(&s1, actor.Scheduler_Config{num_workers = 2, enable_parking = true})
		err2 := actor.init(&s2, actor.Scheduler_Config{num_workers = 2, enable_parking = true})
		testing.expect_value(t, err1, mem.Allocator_Error.None)
		testing.expect_value(t, err2, mem.Allocator_Error.None)
		defer actor.destroy(&s1)
		defer actor.destroy(&s2)

		S2_State :: struct {
			sum:       i64,
			seen_s2:   bool,
			bad_sched: bool,
		}
		s2_sink := actor.spawn(&s2, S2_State{seen_s2 = true}, proc(ctx: ^actor.Context, st: ^S2_State, val: ^i64) {
			st.sum += val^
			if ctx.sched != actor.current_context().sched || ctx.sched == nil {
				st.bad_sched = true
			}
		})

		Relay_State :: struct {
			s2_target: actor.PID(i64),
			s2_ptr:    ^actor.Scheduler,
		}
		s1_relay := actor.spawn(&s1, Relay_State{s2_target = actor.pid(s2_sink), s2_ptr = &s2}, proc(ctx: ^actor.Context, st: ^Relay_State, val: ^i64) {
			// Cross-scheduler send + cross-scheduler forward
			actor.send(ctx, st.s2_target, val^)
			actor.forward(ctx, st.s2_target, val^ * 10)
		})

		actor.scheduler_start(&s2)
		actor.scheduler_start(&s1)
		actor.send(&s1, s1_relay, i64(5))
		actor.wait_idle(&s1)
		actor.wait_idle(&s2)

		testing.expect_value(t, s2_sink.state.sum, i64(55))
		testing.expect(t, !s2_sink.state.bad_sched)

		// 4. Dekker `wait_idle` / unschedule race stress with `enable_parking = true`.
		dekker_sched: actor.Scheduler
		err_d := actor.init(&dekker_sched, actor.Scheduler_Config{num_workers = 4, batch_quota = 4, enable_parking = true})
		testing.expect_value(t, err_d, mem.Allocator_Error.None)
		defer actor.destroy(&dekker_sched)
		actor.scheduler_start(&dekker_sched)

		dekker_sink := actor.spawn(&dekker_sched, i64(0), proc(s: ^i64, x: i64) {
			s^ += x
		})
		EXPECTED_ITER :: 500
		for i := 0; i < EXPECTED_ITER; i += 1 {
			actor.send(&dekker_sched, dekker_sink, i64(1))
			if i % 5 == 0 {
				actor.wait_idle(&dekker_sched)
			}
		}
		actor.wait_idle(&dekker_sched)
		testing.expect_value(t, dekker_sink.state, i64(EXPECTED_ITER))
	})
}

@(test)
test_24_multi_worker_cooperative_call_saturation_and_cycle_guard :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		// 1. 4-worker N-to-1 `actor.call` saturation when the shared target already has
		//    pre-queued async work in `s.shards` and all 4 workers concurrently enter `actor.call`.
		{
			sched: actor.Scheduler
			err := actor.init(&sched, actor.Scheduler_Config{num_workers = 4, batch_quota = 64, enable_parking = true})
			testing.expect_value(t, err, mem.Allocator_Error.None)
			defer actor.destroy(&sched)

			Service_State :: struct {
				hits: int,
			}
			service := actor.spawn(&sched, Service_State{}, proc(ctx: ^actor.Context, st: ^Service_State, x: ^i64) {
				st.hits += 1
				actor.reply(ctx, x^ * 2)
			})

			Producer_State :: struct {
				svc:   actor.PID(i64),
				sum:   i64,
				count: int,
			}
			producers: [4]^actor.Actor(Producer_State, i64)
			for i := 0; i < 4; i += 1 {
				producers[i] = actor.spawn(&sched, Producer_State{svc = actor.pid(service)}, proc(ctx: ^actor.Context, st: ^Producer_State, x: ^i64) {
					res := actor.call(ctx.sched, st.svc, x^, i64)
					st.sum += res
					st.count += 1
					actor.complete_token(ctx, 1)
				})
			}

			// Pre-queue async work onto `service` before starting workers so `service.scheduled == 1`
			// inside `s.shards` while all 4 workers concurrently saturate `actor.call`.
			actor.send(&sched, service, i64(7))
			REQS_PER_PRODUCER :: 64
			intrinsics.atomic_store_explicit(&sched.pending_tokens, i64(4 * REQS_PER_PRODUCER), .Release)
			for m := 0; m < REQS_PER_PRODUCER; m += 1 {
				for p := 0; p < 4; p += 1 {
					actor.send(&sched, producers[p], i64(m + 1), shard_hint = p)
				}
			}

			actor.scheduler_start(&sched)
			actor.wait_tokens(&sched)
			actor.wait_idle(&sched)

			testing.expect_value(t, service.state.hits, 1 + 4 * REQS_PER_PRODUCER)
			expected_sum := i64(REQS_PER_PRODUCER * (REQS_PER_PRODUCER + 1)) // 2 * sum(1..64)
			for p := 0; p < 4; p += 1 {
				testing.expect_value(t, producers[p].state.count, REQS_PER_PRODUCER)
				testing.expect_value(t, producers[p].state.sum, expected_sum)
			}
		}

		// 2. Acyclic `B -> A -> C` DAG under cooperative `actor.call` (1-worker & 2-worker)
		//    plus concurrent `A -> C` while `C` is busy in `step_proc` and `B` calls `A`.
		for workers in ([2]int{1, 2}) {
			dag_sched: actor.Scheduler
			err := actor.init(&dag_sched, actor.Scheduler_Config{num_workers = workers, enable_parking = (workers > 1)})
			testing.expect_value(t, err, mem.Allocator_Error.None)
			defer actor.destroy(&dag_sched)

			C_State :: struct {
				busy_flag: ^u32,
				hits:      int,
			}
			busy_flag: u32 = 0
			actor_c := actor.spawn(&dag_sched, C_State{busy_flag = &busy_flag}, proc(ctx: ^actor.Context, st: ^C_State, x: ^i64) {
				st.hits += 1
				if x^ < 0 {
					// Hold `C` briefly in `step_proc` while `A` starts and `B` is queued.
					intrinsics.atomic_store_explicit(st.busy_flag, 1, .Release)
					for spin := 0; spin < 10_000; spin += 1 {
						intrinsics.cpu_relax()
					}
					return
				}
				actor.reply(ctx, x^ * 3)
			})

			A_State :: struct {
				c_pid: actor.PID(i64),
				res:   i64,
				sum:   i64,
			}
			actor_a := actor.spawn(&dag_sched, A_State{c_pid = actor.pid(actor_c)}, proc(ctx: ^actor.Context, st: ^A_State, x: ^i64) {
				val := actor.call(ctx.sched, st.c_pid, x^, i64)
				st.res = val
				st.sum += val
				actor.reply(ctx, val + 1)
			})

			B_State :: struct {
				a_pid: actor.PID(i64),
				res:   i64,
			}
			actor_b := actor.spawn(&dag_sched, B_State{a_pid = actor.pid(actor_a)}, proc(ctx: ^actor.Context, st: ^B_State, x: ^i64) {
				st.res = actor.call(ctx.sched, st.a_pid, x^, i64)
			})

			actor.scheduler_start(&dag_sched)

			// Direct `B -> A -> C` cooperative DAG chain.
			actor.send(&dag_sched, actor_b, i64(10))
			actor.wait_idle(&dag_sched)
			testing.expect_value(t, actor_b.state.res, i64(31))

			// Concurrent `A -> C` while `C` is actively executing on a peer worker and `B` calls `A`.
			if workers == 2 {
				actor_b.state.res = 0
				actor_a.state.res = 0
				actor_a.state.sum = 0
				intrinsics.atomic_store_explicit(&busy_flag, 0, .Release)
				actor.send(&dag_sched, actor_c, i64(-1), shard_hint = 0)
				for intrinsics.atomic_load_explicit(&busy_flag, .Acquire) == 0 {
					intrinsics.cpu_relax()
				}
				actor.send(&dag_sched, actor_a, i64(20), shard_hint = 1)
				actor.send(&dag_sched, actor_b, i64(5), shard_hint = 0)
				actor.wait_idle(&dag_sched)
				testing.expect_value(t, actor_a.state.sum, i64(75))
				testing.expect_value(t, actor_b.state.res, i64(16))
			}
		}

		// 3. Multi-worker cooperative `A -> B -> A` cycle detection must abort cleanly without hanging.
		{
			sched2: actor.Scheduler
			err2 := actor.init(&sched2, actor.Scheduler_Config{num_workers = 2, enable_parking = false})
			testing.expect_value(t, err2, mem.Allocator_Error.None)
			defer actor.destroy(&sched2)

			Mutual_State_2W :: struct {
				peer_pid: actor.PID(i64),
				caught:   bool,
			}
			actor_a: ^actor.Actor(Mutual_State_2W, i64)
			actor_b := actor.spawn(&sched2, Mutual_State_2W{}, proc(ctx: ^actor.Context, st: ^Mutual_State_2W, x: ^i64) {
				prev_proc := context.assertion_failure_proc
				context.assertion_failure_proc = test22_trap_proc
				jb: libc.jmp_buf
				_test22_jmp = &jb
				_test22_caught = 0
				if libc.setjmp(&jb) == 0 {
					_ = actor.call(st.peer_pid, x^ + 1, i64)
				} else {
					st.caught = _test22_caught > 0
				}
				_test22_jmp = nil
				context.assertion_failure_proc = prev_proc
				actor.reply(ctx, i64(-7))
			})
			actor_a = actor.spawn(&sched2, Mutual_State_2W{peer_pid = actor.pid(actor_b)}, proc(st: ^Mutual_State_2W, x: i64) {
				if x == 10 {
					res := actor.call(st.peer_pid, x, i64)
					st.caught = res == -7
				} else {
					actor.reply(x)
				}
			})
			actor_b.state.peer_pid = actor.pid(actor_a)

			actor.scheduler_start(&sched2)
			actor.send(&sched2, actor_a, i64(10))
			actor.wait_idle(&sched2)

			testing.expect(t, actor_b.state.caught)
			testing.expect(t, actor_a.state.caught)
		}
	})
}
