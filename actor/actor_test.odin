package actor

import "base:intrinsics"
import "core:mem"
import "core:testing"

// Helper to run a test closure inside a `mem.Tracking_Allocator` and assert
// zero memory leaks and zero bad frees.
@(private = "file")
with_tracking_allocator :: proc(t: ^testing.T, body: proc(t: ^testing.T)) {
	track: mem.Tracking_Allocator
	mem.tracking_allocator_init(&track, context.allocator)
	defer mem.tracking_allocator_destroy(&track)

	old_alloc := context.allocator
	context.allocator = mem.tracking_allocator(&track)
	body(t)
	context.allocator = old_alloc

	testing.expect_value(t, len(track.allocation_map), 0)
	testing.expect_value(t, len(track.bad_free_array), 0)
}

@(private = "file")
Test_Node :: struct {
	next: ^Test_Node,
	val:  int,
}

@(test)
test_mpsc_queue_detached_and_chain :: proc(t: ^testing.T) {
	with_tracking_allocator(t, proc(t: ^testing.T) {
		q: MPSC_Queue(Test_Node)
		init(&q)
		testing.expect(t, mpsc_is_empty(&q))

		nodes := make([]Test_Node, 8)
		defer delete(nodes)

		for i := 0; i < 4; i += 1 {
			nodes[i].val = i + 1
			push(&q, &nodes[i])
		}
		testing.expect(t, !mpsc_is_empty(&q))

		// Link nodes[4..7] into a chain and splice via `mpsc_push_chain`
		for i := 4; i < 8; i += 1 {
			nodes[i].val = i + 1
			if i < 7 {
				nodes[i].next = &nodes[i + 1]
			}
		}
		mpsc_push_chain(&q, &nodes[4], &nodes[7])

		for expected := 1; expected <= 8; expected += 1 {
			testing.expect(t, !mpsc_is_empty(&q))
			n := mpsc_pop_detached(&q)
			testing.expect(t, n != nil)
			if n != nil {
				testing.expect_value(t, n.val, expected)
			}
		}
		testing.expect(t, mpsc_is_empty(&q))
		testing.expect(t, mpsc_pop_detached(&q) == nil)
	})
}

@(test)
test_mpsc_queue_dummy_pop :: proc(t: ^testing.T) {
	with_tracking_allocator(t, proc(t: ^testing.T) {
		q: MPSC_Queue(Test_Node)
		init(&q)
		q.stub.val = -1

		n1 := Test_Node{val = 10}
		n2 := Test_Node{val = 20}
		push(&q, &n1)
		push(&q, &n2)

		msg1, ret1, ok1 := mpsc_pop_dummy(&q)
		testing.expect(t, ok1)
		testing.expect_value(t, msg1.val, 10)
		testing.expect_value(t, ret1.val, -1) // Initial stub retired

		msg2, ret2, ok2 := mpsc_pop_dummy(&q)
		testing.expect(t, ok2)
		testing.expect_value(t, msg2.val, 20)
		testing.expect_value(t, ret2.val, 10) // n1 retired when n2 popped

		_, _, ok3 := mpsc_pop_dummy(&q)
		testing.expect(t, !ok3)
	})
}

@(test)
test_spsc_lane_and_sharded_mailbox :: proc(t: ^testing.T) {
	with_tracking_allocator(t, proc(t: ^testing.T) {
		lane := new(SPSC_Lane(int, 16))
		defer free(lane)

		for i := 0; i < 16; i += 1 {
			testing.expect(t, spsc_try_push(lane, i * 10))
		}
		// Lane is now at capacity (16)
		testing.expect(t, !spsc_try_push(lane, 999))

		for i := 0; i < 8; i += 1 {
			v, ok := spsc_try_pop(lane)
			testing.expect(t, ok)
			testing.expect_value(t, v, i * 10)
		}

		// Push 8 more across the wrap-around boundary
		for i := 16; i < 24; i += 1 {
			testing.expect(t, spsc_try_push(lane, i * 10))
		}

		sum := 0
		consumed := spsc_consume_batch(lane, 32, &sum, proc(acc: ^int, msg: ^int) {
			acc^ += msg^
		})
		testing.expect_value(t, consumed, 16)
		// Sum of 80..230 = (80 + 230) * 16 / 2 = 2480
		testing.expect_value(t, sum, 2480)

		// Test Sharded_SPSC_Mailbox
		mbox: Sharded_SPSC_Mailbox(i64, 64)
		init(&mbox, 4)
		defer destroy(&mbox)

		for p := 0; p < 4; p += 1 {
			for k := 0; k < 10; k += 1 {
				sharded_spsc_push(&mbox, p, i64(p + 1))
			}
		}

		total_score: i64 = 0
		count := sharded_spsc_consume(&mbox, 16, &total_score, proc(acc: ^i64, msg: ^i64) {
			acc^ += msg^
		})
		testing.expect_value(t, count, 40)
		testing.expect_value(t, total_score, i64((1 + 2 + 3 + 4) * 10))
	})
}

@(test)
test_envelope_pool_and_batch_collector :: proc(t: ^testing.T) {
	with_tracking_allocator(t, proc(t: ^testing.T) {
		pools := make([]Envelope_Pool(int), 2)
		init(&pools[0], 0, 8)
		init(&pools[1], 1, 8)
		defer {
			destroy(&pools[0])
			destroy(&pools[1])
			delete(pools)
		}

		collector: Batch_Return_Collector(int)
		for i := 0; i < 8; i += 1 {
			e0 := pool_acquire(&pools[0])
			e0.msg = i
			collector_add(&collector, e0)

			e1 := pool_acquire(&pools[1])
			e1.msg = i * 2
			collector_add(&collector, e1)
		}

		// Flush both pools via cross-thread `remote_returns` chain splice
		collector_flush(&collector, pools, -1)

		// Now acquire 8 more from each pool (must harvest from `remote_returns`)
		for i := 0; i < 8; i += 1 {
			e0 := pool_acquire(&pools[0])
			testing.expect(t, e0 != nil)
			e1 := pool_acquire(&pools[1])
			testing.expect(t, e1 != nil)
		}
	})
}

@(test)
test_watermark_slab_fifo_and_chain :: proc(t: ^testing.T) {
	with_tracking_allocator(t, proc(t: ^testing.T) {
		wm: Producer_Watermark
		slab: Watermark_Slab(i64, 16)
		init(&slab, 0)
		defer destroy(&slab)

		for i := 0; i < 10; i += 1 {
			slot := watermark_slab_acquire(&slab, &wm)
			slot.msg = i64(i)
		}
		// Retire those 10 slots so the 16-slot ring can wrap around cleanly
		intrinsics.atomic_add_explicit(&wm.retired_count, 10, .Release)

		first, last := watermark_slab_acquire_chain(&slab, &wm, 8, 42)
		testing.expect(t, first != nil && last != nil)
		testing.expect_value(t, first.msg, 42)
		testing.expect_value(t, last.msg, 42)
	})
}

@(private = "file")
Ping_Msg :: struct {
	val:      i64,
	reply_to: ^Actor(Pong_State, i64),
}

@(private = "file")
Ping_State :: struct {
	transforms: i64,
}

@(private = "file")
Pong_State :: struct {
	sum:      i64,
	received: int,
}

@(test)
test_scheduler_heterogeneous_actors_and_futex_parking :: proc(t: ^testing.T) {
	with_tracking_allocator(t, proc(t: ^testing.T) {
		sched: Scheduler
		init(&sched, Scheduler_Config{
			num_workers    = 2,
			num_shards     = 2,
			batch_quota    = 16,
			enable_parking = true,
		})
		defer destroy(&sched)

		ping_pools := make([]Envelope_Pool(Ping_Msg), 2)
		pong_pools := make([]Envelope_Pool(i64), 2)
		for w := 0; w < 2; w += 1 {
			init(&ping_pools[w], u16(w), 64)
			init(&pong_pools[w], u16(w), 64)
		}
		defer {
			for w := 0; w < 2; w += 1 {
				destroy(&ping_pools[w])
				destroy(&pong_pools[w])
			}
			delete(ping_pools)
			delete(pong_pools)
		}

		ping_actor := spawn(
			Ping_State{},
			proc(ctx: ^Context, self: ^Actor(Ping_State, Ping_Msg), msg: ^Ping_Msg) {
				self.state.transforms += 1
				send(ctx, msg.reply_to, msg.val * 2)
			},
			ping_pools,
		)
		defer destroy(ping_actor)

		pong_actor := spawn(
			Pong_State{},
			proc(ctx: ^Context, self: ^Actor(Pong_State, i64), msg: ^i64) {
				self.state.sum += msg^
				self.state.received += 1
				complete_token(ctx, 1)
			},
			pong_pools,
		)
		defer destroy(pong_actor)

		sched.pending_tokens = 50
		scheduler_start(&sched)

		for i := 1; i <= 50; i += 1 {
			send(&sched, &ping_pools[0], ping_actor, Ping_Msg{
				val      = i64(i),
				reply_to = pong_actor,
			})
		}

		scheduler_wait_tokens(&sched)
		scheduler_wait_idle(&sched)

		testing.expect_value(t, ping_actor.state.transforms, 50)
		testing.expect_value(t, pong_actor.state.received, 50)
		// Sum of 2 * (1..50) = 50 * 51 = 2550
		testing.expect_value(t, pong_actor.state.sum, 2550)
	})
}

@(private = "file")
Ring_Token :: struct {
	accum:     u64,
	hops_left: u32,
}

@(private = "file")
Ring_State :: struct {
	visited: int,
	ring:    []Actor(Ring_State, Ring_Token),
}

@(test)
test_actor_zero_copy_iso_forward_graph :: proc(t: ^testing.T) {
	with_tracking_allocator(t, proc(t: ^testing.T) {
		sched: Scheduler
		init(&sched, Scheduler_Config{
			num_workers    = 2,
			num_shards     = 2,
			batch_quota    = 32,
			enable_parking = false,
		})
		defer destroy(&sched)

		num_actors :: 16
		actors := make([]Actor(Ring_State, Ring_Token), num_actors)
		defer delete(actors)

		for i := 0; i < num_actors; i += 1 {
			actor_init(
				&actors[i],
				Ring_State{visited = 0, ring = actors},
				proc(ctx: ^Context, self: ^Actor(Ring_State, Ring_Token), msg: ^Ring_Token) {
					self.state.visited += 1
					if msg.hops_left > 1 {
						next_id := (self.id + 1) & (num_actors - 1)
						forward(ctx, &self.state.ring[next_id], Ring_Token{
							accum     = msg.accum + 1,
							hops_left = msg.hops_left - 1,
						})
					} else {
						complete_token(ctx, 1)
					}
				},
				nil,
				u32(i),
			)
		}

		tokens := make([]Envelope(Ring_Token), 8)
		defer delete(tokens)

		sched.pending_tokens = 8
		for i := 0; i < 8; i += 1 {
			tokens[i].flags = ENVELOPE_FLAG_STATIC
			tokens[i].msg = Ring_Token{
				accum     = 0,
				hops_left = 16,
			}
			send(&sched, &actors[i], &tokens[i], i % 2)
		}

		scheduler_start(&sched)
		scheduler_wait_tokens(&sched)
		scheduler_wait_idle(&sched)

		total_visits := 0
		for i := 0; i < num_actors; i += 1 {
			total_visits += actors[i].state.visited
		}
		testing.expect_value(t, total_visits, 8 * 16)
	})
}

@(test)
test_send_batch_chain_from_pool :: proc(t: ^testing.T) {
	with_tracking_allocator(t, proc(t: ^testing.T) {
		sched: Scheduler
		init(&sched, Scheduler_Config{
			num_workers    = 1,
			num_shards     = 1,
			batch_quota    = 64,
			enable_parking = false,
		})
		defer destroy(&sched)
		sched.target_messages = 128

		pools := make([]Envelope_Pool(i64), 1)
		init(&pools[0], 0, 64)
		defer {
			destroy(&pools[0])
			delete(pools)
		}

		acc := spawn(
			Pong_State{},
			proc(ctx: ^Context, self: ^Actor(Pong_State, i64), msg: ^i64) {
				self.state.sum += msg^
				self.state.received += 1
			},
			pools,
		)
		defer destroy(acc)

		scheduler_start(&sched)

		batch := [16]i64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		for b := 0; b < 8; b += 1 {
			send(&sched, &pools[0], acc, batch[:])
		}

		scheduler_wait_messages(&sched, 128)
		scheduler_wait_idle(&sched)

		testing.expect_value(t, acc.state.received, 128)
		// Sum of 1..16 = 136; 136 * 8 = 1088
		testing.expect_value(t, acc.state.sum, 1088)
	})
}
