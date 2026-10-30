// =============================================================================
// Example & Verification Benchmark for the `actor` Package
//
// Demonstrates:
//   1. Ergonomic heterogeneous Actors (`Counter_Actor` + `Logger_Actor`) running
//      on the same M:N `actor.Scheduler` with adaptive Futex parking.
//   2. Autonomous 1,024-Actor Graph with `actor.forward` (`iso` zero-copy token
//      passing) and `actor.complete_token` using the reusable library API.
//   3. 4-Producer -> 1,024-Actor Swarm using `actor.Envelope_Pool` (`snmalloc`
//      batched chain return).
//
// Run:
//   odin run examples/library_demo.odin -file -o:speed
// =============================================================================

package main

import "../actor"
import "core:fmt"
import "core:sync"
import "core:thread"
import "core:time"

// -----------------------------------------------------------------------------
// Demo 1: Heterogeneous Actors (Counter + Aggregator) on One Shared Scheduler
// -----------------------------------------------------------------------------

Add_Msg :: struct {
	amount:   i64,
	reply_to: ^actor.Actor(Aggregator_State, Report_Msg),
}

Report_Msg :: struct {
	worker_id: u32,
	subtotal:  i64,
}

Counter_State :: struct {
	count: i64,
}

Aggregator_State :: struct {
	total:   i64,
	reports: int,
}

counter_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Counter_State, Add_Msg), msg: ^Add_Msg) {
	self.state.count += msg.amount
	if msg.reply_to != nil {
		// Send a typed message to a completely different Actor type on the same scheduler!
		actor.send(ctx, msg.reply_to, Report_Msg{
			worker_id = self.id,
			subtotal  = self.state.count,
		})
	}
}

aggregator_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Aggregator_State, Report_Msg), msg: ^Report_Msg) {
	self.state.total += msg.subtotal
	self.state.reports += 1
	actor.complete_token(ctx, 1)
}

run_heterogeneous_demo :: proc() {
	fmt.println("=== 1. Heterogeneous Actor Demo (Counter_Actor -> Aggregator_Actor) ===")

	sched: actor.Scheduler
	actor.scheduler_init(&sched, {
		num_workers    = 2,
		enable_parking = true, // 0% idle CPU when waiting
	})
	defer actor.scheduler_stop_and_destroy(&sched)

	// Create per-worker pools for both message types
	add_pools := make([]actor.Envelope_Pool(Add_Msg), 2)
	rep_pools := make([]actor.Envelope_Pool(Report_Msg), 2)
	for w := 0; w < 2; w += 1 {
		actor.pool_init(&add_pools[w], u16(w), 128)
		actor.pool_init(&rep_pools[w], u16(w), 128)
	}
	defer {
		for w := 0; w < 2; w += 1 {
			actor.pool_destroy(&add_pools[w])
			actor.pool_destroy(&rep_pools[w])
		}
		delete(add_pools)
		delete(rep_pools)
	}

	aggregator := actor.spawn(Aggregator_State{}, aggregator_receive, rep_pools, id = 99)
	defer free(aggregator)

	counters := make([]actor.Actor(Counter_State, Add_Msg), 4)
	defer delete(counters)
	for i := 0; i < 4; i += 1 {
		actor.actor_init(&counters[i], Counter_State{}, counter_receive, add_pools, id = u32(i))
	}

	sched.pending_tokens = 4
	actor.scheduler_start(&sched)

	// Send 10 increments to each counter, followed by a report request
	for i := 0; i < 4; i += 1 {
		for k := 0; k < 10; k += 1 {
			actor.send_from_pool(&sched, &add_pools[0], &counters[i], Add_Msg{amount = 5, reply_to = nil})
		}
		actor.send_from_pool(&sched, &add_pools[0], &counters[i], Add_Msg{amount = 0, reply_to = aggregator})
	}

	actor.scheduler_wait_tokens(&sched)
	fmt.printfln("Aggregator received %d reports | Total score = %d (expected 200)\n",
		aggregator.state.reports, aggregator.state.total)
}

// -----------------------------------------------------------------------------
// Demo 2: Workload 5 Ported to `actor` Library (1,024 Actors, Zero-Copy `forward`)
// -----------------------------------------------------------------------------

NUM_SWARM_ACTORS :: 1024
SWARM_MASK       :: NUM_SWARM_ACTORS - 1
A2A_TOTAL_HOPS   :: 500_000
A2A_INIT_TOKENS  :: 8192
A2A_CPU_ITERS    :: 64

Token_Msg :: struct {
	payload:   u64,
	hops_left: u32,
}

Swarm_State :: struct {
	hash_state: u64,
	peers:      []actor.Actor(Swarm_State, Token_Msg),
}

simulate_cpu_work :: #force_inline proc(seed: u64) -> u64 {
	z := seed
	for i := 0; i < A2A_CPU_ITERS; i += 1 {
		z += 0x9e3779b97f4a7c15
		z = (z ~ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ~ (z >> 27)) * 0x94d049bb133111eb
		z = z ~ (z >> 31)
	}
	return z
}

swarm_actor_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Swarm_State, Token_Msg), msg: ^Token_Msg) {
	res := simulate_cpu_work(msg.payload ~ self.state.hash_state)
	self.state.hash_state ~= res

	if msg.hops_left > 1 {
		target_id := (self.id * 31 + u32(res)) & SWARM_MASK
		target := &self.state.peers[target_id]
		// Zero-copy `iso` forward! Reuses the active `Envelope(Token_Msg)` in-place:
		actor.forward(ctx, target, Token_Msg{
			payload   = res,
			hops_left = msg.hops_left - 1,
		})
	} else {
		actor.complete_token(ctx, 1)
	}
}

bench_library_a2a :: proc(num_workers: int) -> (time.Duration, u64) {
	sched: actor.Scheduler
	actor.scheduler_init(&sched, {
		num_workers    = num_workers,
		num_shards     = num_workers,
		batch_quota    = 64,
		enable_parking = false,
	})
	defer actor.scheduler_stop_and_destroy(&sched)

	actors := make([]actor.Actor(Swarm_State, Token_Msg), NUM_SWARM_ACTORS)
	defer delete(actors)
	for i := 0; i < NUM_SWARM_ACTORS; i += 1 {
		actor.actor_init(
			&actors[i],
			Swarm_State{hash_state = 0, peers = actors},
			swarm_actor_receive,
			nil,
			u32(i),
		)
	}

	tokens := make([]actor.Envelope(Token_Msg), A2A_INIT_TOKENS)
	defer delete(tokens)

	base_hops := A2A_TOTAL_HOPS / A2A_INIT_TOKENS
	extra     := A2A_TOTAL_HOPS % A2A_INIT_TOKENS
	sched.pending_tokens = A2A_INIT_TOKENS

	for i := 0; i < A2A_INIT_TOKENS; i += 1 {
		tokens[i].flags = actor.ENVELOPE_FLAG_STATIC
		tokens[i].msg = Token_Msg{
			payload   = u64(i + 1),
			hops_left = u32(base_hops + (1 if i < extra else 0)),
		}
		target := &actors[u32(i) & SWARM_MASK]
		actor.send_envelope(&sched, target, &tokens[i], i % num_workers)
	}

	start := time.tick_now()
	actor.scheduler_start(&sched)
	actor.scheduler_wait_tokens(&sched)
	elapsed := time.tick_since(start)

	checksum: u64 = 0
	for i := 0; i < NUM_SWARM_ACTORS; i += 1 {
		checksum ~= actors[i].state.hash_state
	}
	return elapsed, checksum
}

// -----------------------------------------------------------------------------
// Demo 3: Workload 4 Ported to `actor` Library (Pooled 4-Producer -> 1,024 Swarm)
// -----------------------------------------------------------------------------

Swarm_Point_State :: struct {
	score: i64,
}

point_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Swarm_Point_State, i64), msg: ^i64) {
	self.state.score += msg^
}

Pooled_Prod_Ctx :: struct {
	sched:  ^actor.Scheduler,
	pool:   ^actor.Envelope_Pool(i64),
	actors: []actor.Actor(Swarm_Point_State, i64),
	count:  int,
	seed:   u32,
	wg:     ^sync.Wait_Group,
}

pooled_prod_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Pooled_Prod_Ctx)t.data
	for i := 0; i < ctx.count; i += 1 {
		target := &ctx.actors[(ctx.seed + u32(i)) & SWARM_MASK]
		actor.send_from_pool(ctx.sched, ctx.pool, target, 1)
	}
	sync.wait_group_done(ctx.wg)
}

bench_library_pooled_swarm :: proc(total_msgs: int, num_producers: int, num_workers: int) -> (time.Duration, i64) {
	sched: actor.Scheduler
	actor.scheduler_init(&sched, {
		num_workers    = num_workers,
		num_shards     = num_producers,
		batch_quota    = 100,
		enable_parking = false,
	})
	defer actor.scheduler_stop_and_destroy(&sched)
	sched.target_messages = i64(total_msgs)

	pools := make([]actor.Envelope_Pool(i64), num_producers)
	for p := 0; p < num_producers; p += 1 {
		actor.pool_init(&pools[p], u16(p), 8192)
	}
	defer {
		for p := 0; p < num_producers; p += 1 {
			actor.pool_destroy(&pools[p])
		}
		delete(pools)
	}

	actors := make([]actor.Actor(Swarm_Point_State, i64), NUM_SWARM_ACTORS)
	defer delete(actors)
	for i := 0; i < NUM_SWARM_ACTORS; i += 1 {
		actor.actor_init(&actors[i], Swarm_Point_State{}, point_receive, pools, u32(i))
	}

	var_wg: sync.Wait_Group
	sync.wait_group_add(&var_wg, num_producers)

	producers := make([]^thread.Thread, num_producers)
	pctxs     := make([]Pooled_Prod_Ctx, num_producers)
	defer delete(producers)
	defer delete(pctxs)

	msgs_per_prod := total_msgs / num_producers
	start := time.tick_now()
	actor.scheduler_start(&sched)

	for p := 0; p < num_producers; p += 1 {
		pctxs[p] = Pooled_Prod_Ctx{
			sched  = &sched,
			pool   = &pools[p],
			actors = actors,
			count  = msgs_per_prod,
			seed   = u32(p * msgs_per_prod),
			wg     = &var_wg,
		}
		producers[p] = thread.create(pooled_prod_proc)
		producers[p].data = &pctxs[p]
		thread.start(producers[p])
	}

	sync.wait_group_wait(&var_wg)
	actor.scheduler_wait_messages(&sched, i64(total_msgs))
	elapsed := time.tick_since(start)

	for p := 0; p < num_producers; p += 1 {
		thread.destroy(producers[p])
	}

	total_score: i64 = 0
	for i := 0; i < NUM_SWARM_ACTORS; i += 1 {
		total_score += actors[i].state.score
	}
	return elapsed, total_score
}

main :: proc() {
	run_heterogeneous_demo()

	fmt.println("=== 2. Reusable `actor` Library Benchmark Verification ===")
	fmt.println("| Workload (via `package actor`)                   | Total Time  | Throughput    | Latency/Msg  | Checksum |")
	fmt.println("|--------------------------------------------------|-------------|---------------|--------------|----------|")

	sw_t, sw_s := bench_library_pooled_swarm(2_000_000, 4, 4)
	sw_sec := time.duration_seconds(sw_t)
	fmt.printfln("| %-48s | %8.2f ms | %9.2f M/s | %8.1f ns/msg | %d |",
		"1,024-Actor Swarm (Envelope_Pool Chain Return)",
		sw_sec * 1000.0, (2_000_000.0 / sw_sec) / 1e6, (sw_sec * 1e9) / 2_000_000.0, sw_s)

	for w in ([]int{1, 2, 4}) {
		a2a_t, a2a_s := bench_library_a2a(w)
		sec := time.duration_seconds(a2a_t)
		name := fmt.tprintf("Autonomous A2A + CPU (`actor.forward`, %d Workers)", w)
		fmt.printfln("| %-48s | %8.2f ms | %9.2f M/s | %8.1f ns/msg | %d |",
			name, sec * 1000.0, (f64(A2A_TOTAL_HOPS) / sec) / 1e6, (sec * 1e9) / f64(A2A_TOTAL_HOPS), a2a_s & 0x7FFFFFFF)
	}
}
