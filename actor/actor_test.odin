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
			_ = scoped_scheduler(num_workers = 2)

			// Spawn actor with 2-arg `(state: ^State, msg: ^Msg)` handler — no manual pool or defer destroy!
			counter := spawn(Counter_State{}, proc(state: ^Counter_State, msg: ^i64) {
				state.total += msg^
				state.messages += 1
			})

			// Ambient single-message sends (`actor.send(target, msg)`)
			for i: i64 = 1; i <= 10; i += 1 {
				send(counter, i)
			}

			// Ambient batch slice splice (`actor.send(target, batch[:])`)
			batch := [4]i64{10, 20, 30, 40}
			send(counter, batch[:])

			wait_messages(14)
			wait_idle()

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
	reply_to: PID(i64), // Decoupled from the sink actor's private `$State`!
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
			sched := scoped_scheduler(num_workers = 2)
			sched.pending_tokens = 1

			// Sink actor accepts `i64`
			sink := spawn(Counter_State{}, proc(state: ^Counter_State, val: i64) {
				state.total = val
				complete_token() // Ambient 0-arg `complete_token()`!
			})

			// Calculator actor with multi-behavior tagged `union` (`Calc_Msg` by value)
			calc := spawn(Counter_State{}, proc(state: ^Counter_State, msg: Calc_Msg) {
				switch m in msg {
				case Calc_Add:
					state.total += m.amount
				case Calc_Mul:
					state.total *= m.factor
				case Calc_Report:
					send(m.reply_to, state.total) // Ambient send to `PID(i64)`!
				}
			})

			calc_pid := pid(calc)
			send(calc_pid, Calc_Msg(Calc_Add{amount = 7}))
			send(calc_pid, Calc_Msg(Calc_Mul{factor = 6}))
			send(calc_pid, Calc_Msg(Calc_Report{reply_to = pid(sink)}))

			wait_tokens()
			wait_idle()
			reported_val = sink.state.total
		}
		testing.expect_value(t, reported_val, 42)
	})
}

@(private = "file")
Hop_Stage :: struct {
	next_pid: PID(i64),
	sum:      i64,
}

@(test)
test_15_ambient_zero_copy_forward_and_unchanged_forward :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		final_sum: i64 = 0
		{
			sched := scoped_scheduler(num_workers = 2)
			sched.pending_tokens = 10

			on_hop :: proc(state: ^Hop_Stage, msg: ^i64) {
				if state.next_pid.ref != nil {
					// Mutate `msg^` in place and forward the envelope via 1-arg `forward(next_pid)`
					msg^ += 5
					forward(state.next_pid)
				} else {
					state.sum += msg^
					complete_token()
				}
			}

			sink   := spawn(Hop_Stage{}, on_hop)
			stage2 := spawn(Hop_Stage{next_pid = pid(sink)}, on_hop)
			stage1 := spawn(Hop_Stage{next_pid = pid(stage2)}, on_hop)

			for i := 0; i < 10; i += 1 {
				send(stage1, i64(10))
			}

			wait_tokens()
			wait_idle()
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
			_ = scoped_scheduler(num_workers = 2)

			// Spawn a struct that embeds its own `receive` behavior using `base:intrinsics`!
			a := spawn_self(Self_Contained_Actor{
				accum = 100,
				receive = proc(self: ^Self_Contained_Actor, msg: i64) {
					self.accum += msg
				},
			})

			send(a, i64(23))
			send(a, i64(19))

			wait_messages(2)
			wait_idle()
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
			_ = scoped_scheduler(num_workers = 1)

			ledger := spawn(Ledger_State{total = 10}, proc(s: ^Ledger_State, m: Ledger_Msg) {
				switch v in m {
				case Ledger_Add:
					s.total += v.x
				case Ledger_Get_Total:
					// Reply address lives in the Envelope header, NOT inside `Ledger_Get_Total`!
					reply(s.total)
				}
			})

			send(ledger, Ledger_Msg(Ledger_Add{x = 25}))
			send(ledger, Ledger_Msg(Ledger_Add{x = 7}))

			// 1. Synchronous `call` from main thread
			sync_total = call(ledger, Ledger_Msg(Ledger_Get_Total{}), i64)

			// 2. Asynchronous `send(target, msg, reply_to)` forwarding reply to another actor
			receiver := spawn(i64(0), proc(state: ^i64, val: i64) {
				state^ = val
			})
			send(ledger, Ledger_Msg(Ledger_Get_Total{}), pid(receiver))

			wait_messages(5)
			wait_idle()
			async_total = receiver.state
		}

			testing.expect_value(t, sync_total, 42)
		testing.expect_value(t, async_total, 42)
	})
}

// ============================================================================
// TEST 18: Actor-to-Actor `actor.call` (Cooperative Worker Stepping) &
//          All `send(..., reply_to)` / `spawn` Overloads
// ============================================================================

@(private = "file")
Compute_Square :: struct {
	x: i64,
}

@(private = "file")
Orchestrator_Run :: struct {
	worker_pid: PID(Compute_Square),
	sink_actor: ^Actor(i64, i64),
	input:      i64,
}

@(test)
test_18_actor_to_actor_cooperative_call_and_reply_overloads :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		final_sink_val: i64 = 0
		direct_actor_reply_val: i64 = 0
		pid_actor_reply_val: i64 = 0
		{
			// Single worker thread (`num_workers = 1`): when `orchestrator` calls `call(worker_pid, ...)`
			// from inside its handler on Worker 0, `call` MUST cooperatively step `math_worker`
			// on Worker 0 or it would deadlock!
			sched := scoped_scheduler(num_workers = 1)

			// Spawn using 3-arg `(ctx: ^Context, state: ^State, msg: ^Msg)` overload (`spawn_sched_state_auto`)
			math_worker := spawn(sched, i64(0), proc(ctx: ^Context, calls: ^i64, req: ^Compute_Square) {
				calls^ += 1
				reply(ctx, req.x * req.x)
			})

			sink := spawn(sched, i64(0), proc(state: ^i64, val: i64) {
				state^ = val
			})

			orchestrator := spawn(sched, i64(0), proc(state: ^i64, cmd: Orchestrator_Run) {
				// Nested synchronous `actor.call` from inside an actor handler on a 1-worker scheduler!
				sq := call(cmd.worker_pid, Compute_Square{x = cmd.input}, i64)
				state^ = sq + 10
				send(cmd.sink_actor, state^)
			})

			send(orchestrator, Orchestrator_Run{
				worker_pid = pid(math_worker),
				sink_actor = sink,
				input      = 9,
			})

			// Wait until `sink` receives `9*9 + 10 = 91`:
			wait_messages(3)
			wait_idle()
			final_sink_val = sink.state

			// Also test `send(actor, msg, reply_to_actor)` and `send(pid, msg, reply_to_actor)` overloads:
			sink2 := spawn(i64(0), proc(state: ^i64, val: i64) { state^ = val })
			sink3 := spawn(i64(0), proc(state: ^i64, val: i64) { state^ = val })

			send(math_worker, Compute_Square{x = 7}, sink2)
			send(pid(math_worker), Compute_Square{x = 11}, sink3)

			wait_messages(7)
			wait_idle()
			direct_actor_reply_val = sink2.state
			pid_actor_reply_val    = sink3.state
		}

		testing.expect_value(t, final_sink_val, 91)
		testing.expect_value(t, direct_actor_reply_val, 49)
		testing.expect_value(t, pid_actor_reply_val, 121)
	})
}

// ============================================================================
// TEST 19: Batch Send to `PID`, Value-Mutating `forward`, & Explicit `destroy`
//          Inside `scoped_scheduler`
// ============================================================================

@(test)
test_19_batch_send_pid_and_early_actor_destroy :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sum_after_batch: i64 = 0
		forwarded_val:   i64 = 0
		{
			_ = scoped_scheduler(num_workers = 2)

			acc := spawn(i64(0), proc(total: ^i64, x: i64) {
				total^ += x
			})
			acc_pid := pid(acc)

			// 1. Ambient batch send to `PID(i64)` (`send_ambient_batch_pid`)
			b1 := [3]i64{10, 20, 30}
			send(acc_pid, b1[:])

			// 2. Worker-context batch send to `PID(i64)` (`send_ctx_batch_pid`) and 2-arg `forward(pid, new_msg)`
			forwarder := spawn(acc_pid, proc(target_pid: ^PID(i64), x: ^i64) {
				b2 := [2]i64{100, 200}
				send(target_pid^, b2[:])
				// 2-arg ambient `forward(pid, updated_msg)`
				forward(target_pid^, x^ * 10)
			})
			send(forwarder, i64(5))

			wait_messages(6)
			wait_idle()

			sum_after_batch = acc.state
			forwarded_val   = acc.state

			// Explicitly destroy `forwarder` BEFORE `scoped_scheduler` exits to verify
			// `actor_destroy` safely unregisters from `s.owned_actors` without double-free!
			destroy(forwarder)
		}

		// 10 + 20 + 30 + 100 + 200 + (5 * 10) = 410
		testing.expect_value(t, sum_after_batch, 410)
		testing.expect_value(t, forwarded_val, 410)
	})
}




