// =============================================================================
// Odin Actor Model Benchmark Suite (v6 — Complete Suite with Autonomous
// Actor-to-Actor CPU Work Scaling)
//
// Implements all actor architectures and optimizations from:
// https://share.gemini.google/vYW4P9VfPnxa
//
// Workloads included:
//   - WORKLOAD 1: 4-Producer -> 1-Actor Fan-In (2,000,000 messages)
//   - WORKLOAD 2: Cross-Thread 1-to-1 Ping-Pong Latency (100,000 Round-Trips)
//   - WORKLOAD 3: 512-Byte Large Payload Crossover (500,000 messages)
//   - WORKLOAD 4: 1,024-Actor Swarm Scaling (Sharded Work-Stealing M:N Scheduler)
//   - WORKLOAD 5: Autonomous Actor-to-Actor Graph + Simulated CPU Work
//                 (500,000 Hops, 1,024 Actors, 64 SplitMix64 Ops/Hop — No External Producers)
//
// Run pure Odin benchmark:
//   odin run benchmarks/actor_benchmark.odin -file -o:speed
//
// Run with official Pony runtime (`libponyrt` 0.77.2) C FFI comparison:
//   ./benchmarks/build_libponyrt.sh
//   odin run benchmarks/actor_benchmark.odin -file -o:speed \
//     -define:USE_LIBPONYRT=true -extra-linker-flags:"-latomic -lm -ldl"
// =============================================================================

package actor_benchmark

import "../actor"
import hugi "hugi:."
import "base:intrinsics"
import "core:c"
import "core:fmt"
import "core:mem"
import "core:os"
import "core:strconv"
import "core:strings"
import "core:sync"
import "core:sync/chan"
import "core:thread"
import "core:time"

// -----------------------------------------------------------------------------
// Benchmark Configuration
// -----------------------------------------------------------------------------

TOTAL_MESSAGES   :: 2_000_000
LARGE_MESSAGES   :: 500_000
A2A_TOTAL_HOPS   :: 500_000
A2A_INIT_TOKENS  :: 8192
A2A_CPU_ITERS    :: 64
NUM_PRODUCERS    :: 4
NUM_SWARM_ACTORS :: 1024
SWARM_MASK       :: NUM_SWARM_ACTORS - 1
NUM_WORKERS      :: 2
PING_PONG_TRIPS  :: 100_000
PONY_BATCH_QUOTA :: 100
CHAIN_BATCH_LEN  :: 32
RING_SLAB_SIZE   :: 8192
RING_SLAB_MASK   :: RING_SLAB_SIZE - 1
FUTEX_SPIN_LIMIT :: 1024

// =============================================================================
// IMPLEMENTATION 1: Worker Thread + Channel (`core:sync/chan` + `core:thread`)
// =============================================================================

Chan_Msg_Add :: struct {
	points: i64,
}
Chan_Msg_Quit :: struct {}

Chan_Actor_Msg :: union {
	Chan_Msg_Add,
	Chan_Msg_Quit,
}

Chan_Actor :: struct {
	inbox:  chan.Chan(Chan_Actor_Msg),
	score:  i64,
	thread: ^thread.Thread,
	done:   ^sync.Wait_Group,
}

chan_actor_proc :: proc(t: ^thread.Thread) {
	actor := cast(^Chan_Actor)t.data
	for {
		msg, ok := chan.recv(actor.inbox)
		if !ok do break
		switch m in msg {
		case Chan_Msg_Add:
			actor.score += m.points
		case Chan_Msg_Quit:
			sync.wait_group_done(actor.done)
			return
		}
	}
}

Chan_Producer_Ctx :: struct {
	inbox: chan.Chan(Chan_Actor_Msg),
	count: int,
	wg:    ^sync.Wait_Group,
}

chan_producer_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Chan_Producer_Ctx)t.data
	for i := 0; i < ctx.count; i += 1 {
		chan.send(ctx.inbox, Chan_Actor_Msg(Chan_Msg_Add{points = 1}))
	}
	sync.wait_group_done(ctx.wg)
}

bench_chan_actor_fan_in :: proc(total_msgs: int, num_producers: int) -> (time.Duration, i64) {
	done_wg: sync.Wait_Group
	prod_wg: sync.Wait_Group
	sync.wait_group_add(&done_wg, 1)
	sync.wait_group_add(&prod_wg, num_producers)

	actor := new(Chan_Actor)
	defer free(actor)

	inbox, err := chan.create_buffered(chan.Chan(Chan_Actor_Msg), 4096, context.allocator)
	assert(err == .None, "Failed to allocate buffered channel")
	defer chan.destroy(inbox)

	actor.inbox = inbox
	actor.done  = &done_wg
	actor.thread = thread.create(chan_actor_proc)
	actor.thread.data = actor
	defer thread.destroy(actor.thread)

	msgs_per_prod := total_msgs / num_producers
	producers := make([]^thread.Thread, num_producers)
	ctxs      := make([]Chan_Producer_Ctx, num_producers)
	defer delete(producers)
	defer delete(ctxs)

	start := time.tick_now()
	thread.start(actor.thread)

	for p := 0; p < num_producers; p += 1 {
		ctxs[p] = Chan_Producer_Ctx{
			inbox = inbox,
			count = msgs_per_prod,
			wg    = &prod_wg,
		}
		producers[p] = thread.create(chan_producer_proc)
		producers[p].data = &ctxs[p]
		thread.start(producers[p])
	}

	sync.wait_group_wait(&prod_wg)
	chan.send(inbox, Chan_Actor_Msg(Chan_Msg_Quit{}))
	sync.wait_group_wait(&done_wg)
	elapsed := time.tick_since(start)

	for p := 0; p < num_producers; p += 1 {
		thread.destroy(producers[p])
	}

	return elapsed, actor.score
}

Chan_PingPong_Ctx :: struct {
	ch_a:  chan.Chan(i64),
	ch_b:  chan.Chan(i64),
	trips: int,
}

chan_pong_worker :: proc(t: ^thread.Thread) {
	ctx := cast(^Chan_PingPong_Ctx)t.data
	for i := 0; i < ctx.trips; i += 1 {
		v, ok := chan.recv(ctx.ch_a)
		if !ok do break
		chan.send(ctx.ch_b, v + 1)
	}
}

bench_chan_ping_pong :: proc(trips: int) -> (time.Duration, i64) {
	ch_a, err1 := chan.create_buffered(chan.Chan(i64), 64, context.allocator)
	ch_b, err2 := chan.create_buffered(chan.Chan(i64), 64, context.allocator)
	assert(err1 == .None && err2 == .None)
	defer chan.destroy(ch_a)
	defer chan.destroy(ch_b)

	ctx := Chan_PingPong_Ctx{ch_a = ch_a, ch_b = ch_b, trips = trips}
	pong_thread := thread.create(chan_pong_worker)
	pong_thread.data = &ctx
	defer thread.destroy(pong_thread)

	val: i64 = 0
	start := time.tick_now()
	thread.start(pong_thread)

	for i := 0; i < trips; i += 1 {
		chan.send(ch_a, val)
		val, _ = chan.recv(ch_b)
	}
	thread.join(pong_thread)
	elapsed := time.tick_since(start)
	return elapsed, val
}

// =============================================================================
// IMPLEMENTATION 2: Data-Oriented Batch System (Single-Thread & Multi-Thread TLS)
// =============================================================================

Actor_ID :: distinct u32

DOD_Message :: struct {
	target: Actor_ID,
	amount: i64,
}

DOD_Actor :: struct {
	id:    Actor_ID,
	score: i64,
}

DOD_World :: struct {
	actors:   [dynamic]DOD_Actor,
	messages: [dynamic]DOD_Message,
}

dod_world_update :: #force_inline proc(w: ^DOD_World) {
	actors_slice := w.actors[:]
	for msg in w.messages {
		actors_slice[msg.target].score += msg.amount
	}
	clear(&w.messages)
}

bench_dod_single_thread :: proc(total_msgs: int, num_actors: int) -> (time.Duration, i64) {
	world: DOD_World
	reserve(&world.actors, num_actors)
	reserve(&world.messages, 8192)
	defer delete(world.actors)
	defer delete(world.messages)

	for i := 0; i < num_actors; i += 1 {
		append(&world.actors, DOD_Actor{id = Actor_ID(i), score = 0})
	}

	batch_size :: 4096
	mask := u32(num_actors - 1)

	start := time.tick_now()
	sent := 0
	for sent < total_msgs {
		chunk := min(batch_size, total_msgs - sent)
		for i := 0; i < chunk; i += 1 {
			target := Actor_ID(u32(sent + i) & mask)
			append(&world.messages, DOD_Message{target = target, amount = 1})
		}
		dod_world_update(&world)
		sent += chunk
	}
	elapsed := time.tick_since(start)

	total_score: i64 = 0
	for a in world.actors {
		total_score += a.score
	}
	return elapsed, total_score
}

DOD_TLS_Producer_Ctx :: struct {
	buf:  []DOD_Message,
	mask: u32,
	seed: u32,
	wg:   ^sync.Wait_Group,
}

dod_tls_producer_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^DOD_TLS_Producer_Ctx)t.data
	for i := 0; i < len(ctx.buf); i += 1 {
		ctx.buf[i] = DOD_Message{
			target = Actor_ID((ctx.seed + u32(i)) & ctx.mask),
			amount = 1,
		}
	}
	sync.wait_group_done(ctx.wg)
}

bench_dod_multithread_tls :: proc(total_msgs: int, num_producers: int, num_actors: int) -> (time.Duration, i64) {
	actors := make([]DOD_Actor, num_actors)
	all_msgs := make([]DOD_Message, total_msgs)
	defer delete(actors)
	defer delete(all_msgs)

	var_wg: sync.Wait_Group
	sync.wait_group_add(&var_wg, num_producers)

	msgs_per_prod := total_msgs / num_producers
	producers := make([]^thread.Thread, num_producers)
	ctxs      := make([]DOD_TLS_Producer_Ctx, num_producers)
	defer delete(producers)
	defer delete(ctxs)

	start := time.tick_now()
	for p := 0; p < num_producers; p += 1 {
		offset := p * msgs_per_prod
		ctxs[p] = DOD_TLS_Producer_Ctx{
			buf  = all_msgs[offset : offset + msgs_per_prod],
			mask = u32(num_actors - 1),
			seed = u32(offset),
			wg   = &var_wg,
		}
		producers[p] = thread.create(dod_tls_producer_proc)
		producers[p].data = &ctxs[p]
		thread.start(producers[p])
	}
	sync.wait_group_wait(&var_wg)

	for msg in all_msgs {
		actors[msg.target].score += msg.amount
	}
	elapsed := time.tick_since(start)

	for p := 0; p < num_producers; p += 1 {
		thread.destroy(producers[p])
	}

	total_score: i64 = 0
	for a in actors {
		total_score += a.score
	}
	return elapsed, total_score
}

// =============================================================================
// IMPLEMENTATION 3: Self-Implemented Pony-Style Actor in Pure Odin
// =============================================================================

Pony_Odin_Msg :: struct {
	next:   ^Pony_Odin_Msg, // Intrusive next pointer at offset 0 (24 bytes total)
	aux_id: u32,            // Used as `in_use` flag (3b/3g) or `prod_id` (3c/3d/3f)
	points: i64,
}

Producer_Watermark :: struct #align(64) {
	retired_count: u64,
}

Pony_Mailbox :: struct #align(64) {
	head:         ^Pony_Odin_Msg, // Producer shared tail (cache line 0)
	_pad0:        [56]u8,
	tail:         ^Pony_Odin_Msg, // Consumer private dummy head (cache line 1)
	_pad1:        [56]u8,
	initial_stub: Pony_Odin_Msg,  // Initial dummy sentinel node
}

mpsc_init :: proc(q: ^Pony_Mailbox) {
	q.initial_stub.next   = nil
	q.initial_stub.aux_id = 0xFFFFFFFF
	q.head = &q.initial_stub
	q.tail = &q.initial_stub
}

mpsc_push :: #force_inline proc(q: ^Pony_Mailbox, msg: ^Pony_Odin_Msg) {
	intrinsics.atomic_store_explicit(&msg.next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&q.head, msg, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.next, msg, .Release)
}

mpsc_push_chain :: #force_inline proc(q: ^Pony_Mailbox, first: ^Pony_Odin_Msg, last: ^Pony_Odin_Msg) {
	intrinsics.atomic_store_explicit(&last.next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&q.head, last, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.next, first, .Release)
}

mpsc_pop_static :: #force_inline proc(q: ^Pony_Mailbox) -> (^Pony_Odin_Msg, bool) {
	tail := q.tail
	next := intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	if next == nil {
		return nil, false
	}
	q.tail = next
	return next, true
}

mpsc_pop_recycled :: #force_inline proc(q: ^Pony_Mailbox) -> (i64, bool) {
	tail := q.tail
	next := intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	if next == nil {
		return 0, false
	}
	pts := next.points
	q.tail = next
	intrinsics.atomic_store_explicit(&tail.aux_id, 0, .Release)
	return pts, true
}

mpsc_pop_batched :: #force_inline proc(q: ^Pony_Mailbox) -> (pts: i64, retired_prod: u32, ok: bool) {
	tail := q.tail
	next := intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	if next == nil {
		return 0, 0, false
	}
	pts = next.points
	retired_prod = tail.aux_id
	q.tail = next
	return pts, retired_prod, true
}

Pony_Odin_Actor :: struct #align(64) {
	mailbox:    Pony_Mailbox,
	watermarks: [NUM_PRODUCERS]Producer_Watermark,
	park_state: sync.Futex,
	scheduled:  u32,
	sched_next: ^Pony_Odin_Actor,
	score:      i64,
	received:   int,
}

Pony_Recycled_Prod_Ctx :: struct {
	actor: ^Pony_Odin_Actor,
	slab:  []Pony_Odin_Msg,
	count: int,
	wg:    ^sync.Wait_Group,
}

pony_recycled_producer_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Pony_Recycled_Prod_Ctx)t.data
	cursor: u32 = 0
	for i := 0; i < ctx.count; i += 1 {
		slot := &ctx.slab[cursor & RING_SLAB_MASK]
		cursor += 1
		for intrinsics.atomic_load_explicit(&slot.aux_id, .Acquire) != 0 {
			intrinsics.cpu_relax()
		}
		intrinsics.atomic_store_explicit(&slot.aux_id, 1, .Relaxed)
		slot.points = 1
		mpsc_push(&ctx.actor.mailbox, slot)
	}
	sync.wait_group_done(ctx.wg)
}

bench_self_pony_recycled_fan_in :: proc(total_msgs: int, num_producers: int) -> (time.Duration, i64) {
	actor := new(Pony_Odin_Actor)
	defer free(actor)
	mpsc_init(&actor.mailbox)
	actor.mailbox.initial_stub.aux_id = 1

	slabs := make([]Pony_Odin_Msg, num_producers * RING_SLAB_SIZE)
	defer delete(slabs)

	var_wg: sync.Wait_Group
	sync.wait_group_add(&var_wg, num_producers)

	msgs_per_prod := total_msgs / num_producers
	producers := make([]^thread.Thread, num_producers)
	ctxs      := make([]Pony_Recycled_Prod_Ctx, num_producers)
	defer delete(producers)
	defer delete(ctxs)

	start := time.tick_now()
	for p := 0; p < num_producers; p += 1 {
		offset := p * RING_SLAB_SIZE
		ctxs[p] = Pony_Recycled_Prod_Ctx{
			actor = actor,
			slab  = slabs[offset : offset + RING_SLAB_SIZE],
			count = msgs_per_prod,
			wg    = &var_wg,
		}
		producers[p] = thread.create(pony_recycled_producer_proc)
		producers[p].data = &ctxs[p]
		thread.start(producers[p])
	}

	for actor.received < total_msgs {
		processed := 0
		for processed < PONY_BATCH_QUOTA {
			pts, ok := mpsc_pop_recycled(&actor.mailbox)
			if !ok do break
			actor.score += pts
			actor.received += 1
			processed += 1
		}
		if processed == 0 {
			intrinsics.cpu_relax()
		}
	}

	sync.wait_group_wait(&var_wg)
	elapsed := time.tick_since(start)
	for p := 0; p < num_producers; p += 1 {
		thread.destroy(producers[p])
	}
	return elapsed, actor.score
}

Pony_Batched_Prod_Ctx :: struct {
	actor:      ^Pony_Odin_Actor,
	slab:       []Pony_Odin_Msg,
	prod_id:    u32,
	count:      int,
	use_chain:  bool,
	use_parker: bool,
	wg:         ^sync.Wait_Group,
}

pony_batched_producer_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Pony_Batched_Prod_Ctx)t.data
	actor := ctx.actor
	pid := ctx.prod_id
	cursor: u64 = 0
	cached_retired: u64 = 0

	if !ctx.use_chain {
		for i := 0; i < ctx.count; i += 1 {
			for cursor - cached_retired >= (RING_SLAB_SIZE - 2) {
				cached_retired = intrinsics.atomic_load_explicit(&actor.watermarks[pid].retired_count, .Acquire)
				if cursor - cached_retired >= (RING_SLAB_SIZE - 2) {
					intrinsics.cpu_relax()
				}
			}
			slot := &ctx.slab[cursor & RING_SLAB_MASK]
			cursor += 1
			slot.aux_id = pid
			slot.points = 1
			mpsc_push(&actor.mailbox, slot)

			if ctx.use_parker {
				if intrinsics.atomic_load_explicit((^u32)(&actor.park_state), .Acquire) == 1 {
					if intrinsics.atomic_exchange_explicit((^u32)(&actor.park_state), 0, .Acq_Rel) == 1 {
						sync.futex_signal(&actor.park_state)
					}
				}
			}
		}
	} else {
		remaining := ctx.count
		for remaining > 0 {
			k := min(CHAIN_BATCH_LEN, remaining)
			for (cursor + u64(k)) - cached_retired >= (RING_SLAB_SIZE - 2) {
				cached_retired = intrinsics.atomic_load_explicit(&actor.watermarks[pid].retired_count, .Acquire)
				if (cursor + u64(k)) - cached_retired >= (RING_SLAB_SIZE - 2) {
					intrinsics.cpu_relax()
				}
			}
			first := &ctx.slab[cursor & RING_SLAB_MASK]
			prev  := first
			first.aux_id = pid
			first.points = 1
			cursor += 1

			for i := 1; i < k; i += 1 {
				cur := &ctx.slab[cursor & RING_SLAB_MASK]
				cursor += 1
				cur.aux_id = pid
				cur.points = 1
				intrinsics.atomic_store_explicit(&prev.next, cur, .Relaxed)
				prev = cur
			}
			mpsc_push_chain(&actor.mailbox, first, prev)
			remaining -= k
		}
	}
	sync.wait_group_done(ctx.wg)
}

bench_self_pony_batched_fan_in :: proc(total_msgs: int, num_producers: int, use_chain: bool, use_parker: bool) -> (time.Duration, i64) {
	actor := new(Pony_Odin_Actor)
	defer free(actor)
	mpsc_init(&actor.mailbox)

	slabs := make([]Pony_Odin_Msg, num_producers * RING_SLAB_SIZE)
	defer delete(slabs)

	var_wg: sync.Wait_Group
	sync.wait_group_add(&var_wg, num_producers)

	msgs_per_prod := total_msgs / num_producers
	producers := make([]^thread.Thread, num_producers)
	ctxs      := make([]Pony_Batched_Prod_Ctx, num_producers)
	defer delete(producers)
	defer delete(ctxs)

	start := time.tick_now()
	for p := 0; p < num_producers; p += 1 {
		offset := p * RING_SLAB_SIZE
		ctxs[p] = Pony_Batched_Prod_Ctx{
			actor      = actor,
			slab       = slabs[offset : offset + RING_SLAB_SIZE],
			prod_id    = u32(p),
			count      = msgs_per_prod,
			use_chain  = use_chain,
			use_parker = use_parker,
			wg         = &var_wg,
		}
		producers[p] = thread.create(pony_batched_producer_proc)
		producers[p].data = &ctxs[p]
		thread.start(producers[p])
	}

	spin_count := 0
	for actor.received < total_msgs {
		processed := 0
		local_retired: [NUM_PRODUCERS]u64

		for processed < PONY_BATCH_QUOTA {
			pts, retired_prod, ok := mpsc_pop_batched(&actor.mailbox)
			if !ok do break
			actor.score += pts
			if retired_prod < NUM_PRODUCERS {
				local_retired[retired_prod] += 1
			}
			actor.received += 1
			processed += 1
		}

		if processed > 0 {
			spin_count = 0
			for p := 0; p < num_producers; p += 1 {
				if local_retired[p] > 0 {
					intrinsics.atomic_add_explicit(&actor.watermarks[p].retired_count, local_retired[p], .Release)
				}
			}
		} else if !use_parker {
			intrinsics.cpu_relax()
		} else {
			if spin_count < FUTEX_SPIN_LIMIT {
				spin_count += 1
				intrinsics.cpu_relax()
			} else {
				intrinsics.atomic_store_explicit((^u32)(&actor.park_state), 1, .Seq_Cst)
				if intrinsics.atomic_load_explicit(&actor.mailbox.head, .Acquire) == actor.mailbox.tail &&
				   actor.received < total_msgs {
					sync.futex_wait(&actor.park_state, 1)
				} else {
					intrinsics.atomic_store_explicit((^u32)(&actor.park_state), 0, .Relaxed)
				}
				spin_count = 0
			}
		}
	}

	sync.wait_group_wait(&var_wg)
	elapsed := time.tick_since(start)
	for p := 0; p < num_producers; p += 1 {
		thread.destroy(producers[p])
	}
	return elapsed, actor.score
}

SPSC_Lane :: struct #align(64) {
	write_idx:        u64,
	_pad0:            [56]u8,
	read_idx:         u64,
	cached_write_idx: u64,
	_pad1:            [48]u8,
	ring:             [RING_SLAB_SIZE]i64,
}

Sharded_SPSC_Actor :: struct #align(64) {
	lanes:    [NUM_PRODUCERS]SPSC_Lane,
	score:    i64,
	received: int,
}

SPSC_Prod_Ctx :: struct {
	lane:  ^SPSC_Lane,
	count: int,
	wg:    ^sync.Wait_Group,
}

spsc_producer_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^SPSC_Prod_Ctx)t.data
	lane := ctx.lane
	w: u64 = 0
	r_cache: u64 = 0

	for i := 0; i < ctx.count; i += 1 {
		for w - r_cache >= RING_SLAB_SIZE {
			r_cache = intrinsics.atomic_load_explicit(&lane.read_idx, .Acquire)
			if w - r_cache >= RING_SLAB_SIZE {
				intrinsics.cpu_relax()
			}
		}
		lane.ring[w & RING_SLAB_MASK] = 1
		w += 1
		intrinsics.atomic_store_explicit(&lane.write_idx, w, .Release)
	}
	sync.wait_group_done(ctx.wg)
}

bench_sharded_spsc_fan_in :: proc(total_msgs: int, num_producers: int) -> (time.Duration, i64) {
	actor := new(Sharded_SPSC_Actor)
	defer free(actor)

	var_wg: sync.Wait_Group
	sync.wait_group_add(&var_wg, num_producers)

	msgs_per_prod := total_msgs / num_producers
	producers := make([]^thread.Thread, num_producers)
	ctxs      := make([]SPSC_Prod_Ctx, num_producers)
	defer delete(producers)
	defer delete(ctxs)

	start := time.tick_now()
	for p := 0; p < num_producers; p += 1 {
		ctxs[p] = SPSC_Prod_Ctx{
			lane  = &actor.lanes[p],
			count = msgs_per_prod,
			wg    = &var_wg,
		}
		producers[p] = thread.create(spsc_producer_proc)
		producers[p].data = &ctxs[p]
		thread.start(producers[p])
	}

	for actor.received < total_msgs {
		total_batch := 0
		for p := 0; p < num_producers; p += 1 {
			lane := &actor.lanes[p]
			r := intrinsics.atomic_load_explicit(&lane.read_idx, .Relaxed)
			w := lane.cached_write_idx
			if r == w {
				w = intrinsics.atomic_load_explicit(&lane.write_idx, .Acquire)
				lane.cached_write_idx = w
			}
			avail := w - r
			if avail > PONY_BATCH_QUOTA {
				avail = PONY_BATCH_QUOTA
			}
			if avail > 0 {
				for k: u64 = 0; k < avail; k += 1 {
					actor.score += lane.ring[(r + k) & RING_SLAB_MASK]
				}
				r += avail
				intrinsics.atomic_store_explicit(&lane.read_idx, r, .Release)
				actor.received += int(avail)
				total_batch += int(avail)
			}
		}
		if total_batch == 0 {
			intrinsics.cpu_relax()
		}
	}

	sync.wait_group_wait(&var_wg)
	elapsed := time.tick_since(start)
	for p := 0; p < num_producers; p += 1 {
		thread.destroy(producers[p])
	}
	return elapsed, actor.score
}

// =============================================================================
// WORKLOAD 2: Self-Implemented Pony MPSC Ping-Pong
// =============================================================================

MPSC_PingPong_Ctx :: struct #align(64) {
	q_a:       Pony_Mailbox,
	q_b:       Pony_Mailbox,
	replies_b: []Pony_Odin_Msg,
	trips:     int,
}

mpsc_pong_worker :: proc(t: ^thread.Thread) {
	ctx := cast(^MPSC_PingPong_Ctx)t.data
	for i := 0; i < ctx.trips; i += 1 {
		msg: ^Pony_Odin_Msg
		ok: bool
		for {
			msg, ok = mpsc_pop_static(&ctx.q_a)
			if ok do break
			intrinsics.cpu_relax()
		}
		rep := &ctx.replies_b[i]
		rep.points = msg.points + 1
		mpsc_push(&ctx.q_b, rep)
	}
}

bench_self_pony_ping_pong :: proc(trips: int) -> (time.Duration, i64) {
	ctx := new(MPSC_PingPong_Ctx)
	defer free(ctx)
	mpsc_init(&ctx.q_a)
	mpsc_init(&ctx.q_b)
	ctx.trips = trips

	pings_a := make([]Pony_Odin_Msg, trips)
	ctx.replies_b = make([]Pony_Odin_Msg, trips)
	defer delete(pings_a)
	defer delete(ctx.replies_b)

	pong_thread := thread.create(mpsc_pong_worker)
	pong_thread.data = ctx
	defer thread.destroy(pong_thread)

	val: i64 = 0
	start := time.tick_now()
	thread.start(pong_thread)

	for i := 0; i < trips; i += 1 {
		pings_a[i].points = val
		mpsc_push(&ctx.q_a, &pings_a[i])
		for {
			rep, ok := mpsc_pop_static(&ctx.q_b)
			if ok {
				val = rep.points
				break
			}
			intrinsics.cpu_relax()
		}
	}
	thread.join(pong_thread)
	elapsed := time.tick_since(start)
	return elapsed, val
}

// =============================================================================
// WORKLOAD 3: 512-Byte Large Payload Crossover (Zero-Copy `iso` Pointer vs. Copy)
// =============================================================================

Large_Pony_Msg :: struct {
	next:    ^Large_Pony_Msg,
	prod_id: u32,
	data:    [64]i64,
}

Large_Pony_Mailbox :: struct #align(64) {
	head:         ^Large_Pony_Msg,
	_pad0:        [56]u8,
	tail:         ^Large_Pony_Msg,
	_pad1:        [56]u8,
	initial_stub: Large_Pony_Msg,
}

Large_Pony_Actor :: struct #align(64) {
	mailbox:    Large_Pony_Mailbox,
	watermarks: [NUM_PRODUCERS]Producer_Watermark,
	score:      i64,
	received:   int,
}

Large_Pony_Prod_Ctx :: struct {
	actor:   ^Large_Pony_Actor,
	slab:    []Large_Pony_Msg,
	prod_id: u32,
	count:   int,
	wg:      ^sync.Wait_Group,
}

large_pony_producer_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Large_Pony_Prod_Ctx)t.data
	actor := ctx.actor
	pid := ctx.prod_id
	cursor: u64 = 0
	cached_retired: u64 = 0

	for i := 0; i < ctx.count; i += 1 {
		for cursor - cached_retired >= (RING_SLAB_SIZE - 2) {
			cached_retired = intrinsics.atomic_load_explicit(&actor.watermarks[pid].retired_count, .Acquire)
			if cursor - cached_retired >= (RING_SLAB_SIZE - 2) {
				intrinsics.cpu_relax()
			}
		}
		slot := &ctx.slab[cursor & RING_SLAB_MASK]
		cursor += 1
		slot.prod_id = pid
		slot.data[0]  = 1
		slot.data[63] = 1
		intrinsics.atomic_store_explicit(&slot.next, nil, .Relaxed)
		prev := intrinsics.atomic_exchange_explicit(&actor.mailbox.head, slot, .Acq_Rel)
		intrinsics.atomic_store_explicit(&prev.next, slot, .Release)
	}
	sync.wait_group_done(ctx.wg)
}

bench_large_pony_zerocopy :: proc(total_msgs: int, num_producers: int) -> (time.Duration, i64) {
	actor := new(Large_Pony_Actor)
	defer free(actor)
	actor.mailbox.initial_stub.next    = nil
	actor.mailbox.initial_stub.prod_id = 0xFFFFFFFF
	actor.mailbox.head = &actor.mailbox.initial_stub
	actor.mailbox.tail = &actor.mailbox.initial_stub

	slabs := make([]Large_Pony_Msg, num_producers * RING_SLAB_SIZE)
	defer delete(slabs)

	var_wg: sync.Wait_Group
	sync.wait_group_add(&var_wg, num_producers)

	msgs_per_prod := total_msgs / num_producers
	producers := make([]^thread.Thread, num_producers)
	ctxs      := make([]Large_Pony_Prod_Ctx, num_producers)
	defer delete(producers)
	defer delete(ctxs)

	start := time.tick_now()
	for p := 0; p < num_producers; p += 1 {
		offset := p * RING_SLAB_SIZE
		ctxs[p] = Large_Pony_Prod_Ctx{
			actor   = actor,
			slab    = slabs[offset : offset + RING_SLAB_SIZE],
			prod_id = u32(p),
			count   = msgs_per_prod,
			wg      = &var_wg,
		}
		producers[p] = thread.create(large_pony_producer_proc)
		producers[p].data = &ctxs[p]
		thread.start(producers[p])
	}

	for actor.received < total_msgs {
		processed := 0
		local_retired: [NUM_PRODUCERS]u64
		for processed < PONY_BATCH_QUOTA {
			tail := actor.mailbox.tail
			next := intrinsics.atomic_load_explicit(&tail.next, .Acquire)
			if next == nil do break
			actor.score += next.data[0] + next.data[63]
			actor.mailbox.tail = next
			if tail.prod_id < NUM_PRODUCERS {
				local_retired[tail.prod_id] += 1
			}
			actor.received += 1
			processed += 1
		}
		if processed > 0 {
			for p := 0; p < num_producers; p += 1 {
				if local_retired[p] > 0 {
					intrinsics.atomic_add_explicit(&actor.watermarks[p].retired_count, local_retired[p], .Release)
				}
			}
		} else {
			intrinsics.cpu_relax()
		}
	}

	sync.wait_group_wait(&var_wg)
	elapsed := time.tick_since(start)
	for p := 0; p < num_producers; p += 1 {
		thread.destroy(producers[p])
	}
	return elapsed, actor.score
}

// =============================================================================
// WORKLOAD 4: 1,024-Actor Sharded Work-Stealing M:N Scheduler Swarm
// =============================================================================

RunQ_Shard :: struct #align(64) {
	head:     ^Pony_Odin_Actor,
	_pad0:    [56]u8,
	tail:     ^Pony_Odin_Actor,
	_pad1:    [56]u8,
	stub:     Pony_Odin_Actor,
	pop_lock: sync.Ticket_Mutex,
}

shard_init :: proc(q: ^RunQ_Shard) {
	q.stub.sched_next = nil
	q.head = &q.stub
	q.tail = &q.stub
}

shard_push :: #force_inline proc(q: ^RunQ_Shard, a: ^Pony_Odin_Actor) {
	intrinsics.atomic_store_explicit(&a.sched_next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&q.head, a, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.sched_next, a, .Release)
}

shard_pop_or_steal :: proc(q: ^RunQ_Shard) -> ^Pony_Odin_Actor {
	if q.tail == &q.stub && intrinsics.atomic_load_explicit(&q.stub.sched_next, .Acquire) == nil {
		return nil
	}
	sync.ticket_mutex_lock(&q.pop_lock)
	defer sync.ticket_mutex_unlock(&q.pop_lock)

	tail := q.tail
	next := intrinsics.atomic_load_explicit(&tail.sched_next, .Acquire)
	if tail == &q.stub {
		if next == nil do return nil
		q.tail = next
		tail = next
		next = intrinsics.atomic_load_explicit(&tail.sched_next, .Acquire)
	}
	if next != nil {
		q.tail = next
		return tail
	}
	head := intrinsics.atomic_load_explicit(&q.head, .Acquire)
	if tail != head do return nil

	intrinsics.atomic_store_explicit(&q.stub.sched_next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&q.head, &q.stub, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.sched_next, &q.stub, .Release)
	next = intrinsics.atomic_load_explicit(&tail.sched_next, .Acquire)
	for next == nil {
		intrinsics.cpu_relax()
		next = intrinsics.atomic_load_explicit(&tail.sched_next, .Acquire)
	}
	q.tail = next
	return tail
}

WS_Scheduler :: struct #align(64) {
	shards:          [NUM_PRODUCERS]RunQ_Shard,
	total_processed: int,
	target_msgs:     int,
}

WS_Worker_Ctx :: struct {
	sched:     ^WS_Scheduler,
	worker_id: int,
}

ws_worker_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^WS_Worker_Ctx)t.data
	s   := ctx.sched
	wid := ctx.worker_id

	for intrinsics.atomic_load_explicit(&s.total_processed, .Relaxed) < s.target_msgs {
		a: ^Pony_Odin_Actor = nil
		for i := 0; i < NUM_PRODUCERS; i += 1 {
			idx := (wid + i) & (NUM_PRODUCERS - 1)
			a = shard_pop_or_steal(&s.shards[idx])
			if a != nil do break
		}
		if a == nil {
			intrinsics.cpu_relax()
			continue
		}

		processed := 0
		for processed < PONY_BATCH_QUOTA {
			pts, ok := mpsc_pop_recycled(&a.mailbox)
			if !ok do break
			a.score += pts
			processed += 1
		}
		if processed > 0 {
			intrinsics.atomic_add_explicit(&s.total_processed, processed, .Relaxed)
		}

		intrinsics.atomic_store_explicit(&a.scheduled, 0, .Seq_Cst)
		if intrinsics.atomic_load_explicit(&a.mailbox.tail.next, .Seq_Cst) != nil {
			if intrinsics.atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0 {
				shard_push(&s.shards[wid], a)
			}
		}
	}
}

WS_Producer_Ctx :: struct {
	sched:      ^WS_Scheduler,
	actors:     []Pony_Odin_Actor,
	slab:       []Pony_Odin_Msg,
	prod_id:    int,
	num_actors: int,
	count:      int,
	seed:       u32,
	wg:         ^sync.Wait_Group,
}

ws_producer_proc :: proc(t: ^thread.Thread) {
	ctx      := cast(^WS_Producer_Ctx)t.data
	my_shard := &ctx.sched.shards[ctx.prod_id]
	mask     := u32(ctx.num_actors - 1)
	cursor: u32 = 0

	for i := 0; i < ctx.count; i += 1 {
		slot := &ctx.slab[cursor & RING_SLAB_MASK]
		cursor += 1
		for intrinsics.atomic_load_explicit(&slot.aux_id, .Acquire) != 0 {
			intrinsics.cpu_relax()
		}
		intrinsics.atomic_store_explicit(&slot.aux_id, 1, .Relaxed)
		slot.points = 1

		a := &ctx.actors[(ctx.seed + u32(i)) & mask]
		mpsc_push(&a.mailbox, slot)
		if intrinsics.atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0 {
			shard_push(my_shard, a)
		}
	}
	sync.wait_group_done(ctx.wg)
}

bench_work_stealing_swarm :: proc(total_msgs: int, num_producers: int, num_actors: int) -> (time.Duration, i64) {
	sched := new(WS_Scheduler)
	defer free(sched)
	for p := 0; p < NUM_PRODUCERS; p += 1 {
		shard_init(&sched.shards[p])
	}
	sched.target_msgs = total_msgs

	actors := make([]Pony_Odin_Actor, num_actors)
	defer delete(actors)
	for i := 0; i < num_actors; i += 1 {
		mpsc_init(&actors[i].mailbox)
		actors[i].mailbox.initial_stub.aux_id = 1
	}

	slabs := make([]Pony_Odin_Msg, num_producers * RING_SLAB_SIZE)
	defer delete(slabs)

	var_wg: sync.Wait_Group
	sync.wait_group_add(&var_wg, num_producers)

	workers := make([]^thread.Thread, NUM_WORKERS)
	wctxs   := make([]WS_Worker_Ctx, NUM_WORKERS)
	producers := make([]^thread.Thread, num_producers)
	pctxs     := make([]WS_Producer_Ctx, num_producers)
	defer delete(workers)
	defer delete(wctxs)
	defer delete(producers)
	defer delete(pctxs)

	msgs_per_prod := total_msgs / num_producers
	start := time.tick_now()

	for w := 0; w < NUM_WORKERS; w += 1 {
		wctxs[w] = WS_Worker_Ctx{sched = sched, worker_id = w}
		workers[w] = thread.create(ws_worker_proc)
		workers[w].data = &wctxs[w]
		thread.start(workers[w])
	}

	for p := 0; p < num_producers; p += 1 {
		offset := p * RING_SLAB_SIZE
		pctxs[p] = WS_Producer_Ctx{
			sched      = sched,
			actors     = actors,
			slab       = slabs[offset : offset + RING_SLAB_SIZE],
			prod_id    = p,
			num_actors = num_actors,
			count      = msgs_per_prod,
			seed       = u32(p * msgs_per_prod),
			wg         = &var_wg,
		}
		producers[p] = thread.create(ws_producer_proc)
		producers[p].data = &pctxs[p]
		thread.start(producers[p])
	}

	sync.wait_group_wait(&var_wg)
	for w := 0; w < NUM_WORKERS; w += 1 {
		thread.join(workers[w])
		thread.destroy(workers[w])
	}
	elapsed := time.tick_since(start)

	for p := 0; p < num_producers; p += 1 {
		thread.destroy(producers[p])
	}

	total_score: i64 = 0
	for i := 0; i < num_actors; i += 1 {
		total_score += actors[i].score
	}
	return elapsed, total_score
}

// =============================================================================
// WORKLOAD 5: Autonomous Actor-to-Actor Graph + Per-Message CPU Work
//   - 1,024 Actors in a closed autonomous network (NO external producer threads)
//   - 8,192 initial tokens circulating for 500,000 total message hops
//   - Every hop runs 64 rounds of SplitMix64 CPU math and forwards the `iso`
//     token directly to another actor!
// =============================================================================

simulate_cpu_work :: #force_inline proc "contextless" (seed: u64) -> u64 {
	z := seed
	for i := 0; i < A2A_CPU_ITERS; i += 1 {
		z += 0x9e3779b97f4a7c15
		z = (z ~ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ~ (z >> 27)) * 0x94d049bb133111eb
		z = z ~ (z >> 31)
	}
	return z
}

A2A_Token :: struct {
	next:      ^A2A_Token,
	payload:   u64,
	hops_left: u32,
}

A2A_Actor :: struct #align(64) {
	head:       ^A2A_Token,
	_pad0:      [56]u8,
	tail:       ^A2A_Token,
	_pad1:      [56]u8,
	stub:       A2A_Token,
	scheduled:  u32,
	sched_next: ^A2A_Actor,
	id:         u32,
	state:      u64,
}

a2a_actor_init :: proc(a: ^A2A_Actor, id: u32) {
	a.stub.next = nil
	a.head = &a.stub
	a.tail = &a.stub
	a.scheduled = 0
	a.sched_next = nil
	a.id = id
	a.state = 0
}

a2a_push :: #force_inline proc(a: ^A2A_Actor, m: ^A2A_Token) {
	intrinsics.atomic_store_explicit(&m.next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&a.head, m, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.next, m, .Release)
}

a2a_pop_detached :: #force_inline proc(a: ^A2A_Actor) -> ^A2A_Token {
	tail := a.tail
	next := intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	if tail == &a.stub {
		if next == nil do return nil
		a.tail = next
		tail = next
		next = intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	}
	if next != nil {
		a.tail = next
		return tail
	}
	head := intrinsics.atomic_load_explicit(&a.head, .Acquire)
	if tail != head do return nil

	intrinsics.atomic_store_explicit(&a.stub.next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&a.head, &a.stub, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.next, &a.stub, .Release)
	next = intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	for next == nil {
		intrinsics.cpu_relax()
		next = intrinsics.atomic_load_explicit(&tail.next, .Acquire)
	}
	a.tail = next
	return tail
}

A2A_Shard :: struct #align(64) {
	head:     ^A2A_Actor,
	_pad0:    [56]u8,
	tail:     ^A2A_Actor,
	_pad1:    [56]u8,
	stub:     A2A_Actor,
	pop_lock: sync.Ticket_Mutex,
}

a2a_shard_init :: proc(q: ^A2A_Shard) {
	q.stub.sched_next = nil
	q.head = &q.stub
	q.tail = &q.stub
}

a2a_shard_push :: #force_inline proc(q: ^A2A_Shard, a: ^A2A_Actor) {
	intrinsics.atomic_store_explicit(&a.sched_next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&q.head, a, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.sched_next, a, .Release)
}

a2a_shard_pop :: proc(q: ^A2A_Shard) -> ^A2A_Actor {
	if q.tail == &q.stub && intrinsics.atomic_load_explicit(&q.stub.sched_next, .Acquire) == nil {
		return nil
	}
	sync.ticket_mutex_lock(&q.pop_lock)
	defer sync.ticket_mutex_unlock(&q.pop_lock)

	tail := q.tail
	next := intrinsics.atomic_load_explicit(&tail.sched_next, .Acquire)
	if tail == &q.stub {
		if next == nil do return nil
		q.tail = next
		tail = next
		next = intrinsics.atomic_load_explicit(&tail.sched_next, .Acquire)
	}
	if next != nil {
		q.tail = next
		return tail
	}
	head := intrinsics.atomic_load_explicit(&q.head, .Acquire)
	if tail != head do return nil

	intrinsics.atomic_store_explicit(&q.stub.sched_next, nil, .Relaxed)
	prev := intrinsics.atomic_exchange_explicit(&q.head, &q.stub, .Acq_Rel)
	intrinsics.atomic_store_explicit(&prev.sched_next, &q.stub, .Release)
	next = intrinsics.atomic_load_explicit(&tail.sched_next, .Acquire)
	for next == nil {
		intrinsics.cpu_relax()
		next = intrinsics.atomic_load_explicit(&tail.sched_next, .Acquire)
	}
	q.tail = next
	return tail
}

A2A_Scheduler :: struct #align(64) {
	shards:           [16]A2A_Shard,
	actors:           []A2A_Actor,
	num_shards:       int,
	remaining_tokens: int,
}

A2A_Worker_Ctx :: struct {
	sched: ^A2A_Scheduler,
	wid:   int,
}

a2a_worker_proc :: proc(t: ^thread.Thread) {
	ctx      := cast(^A2A_Worker_Ctx)t.data
	s        := ctx.sched
	wid      := ctx.wid
	my_shard := &s.shards[wid]

	for intrinsics.atomic_load_explicit(&s.remaining_tokens, .Acquire) > 0 {
		a := a2a_shard_pop(my_shard)
		if a == nil && s.num_shards > 1 {
			for i := 1; i < s.num_shards; i += 1 {
				peer := (wid + i) % s.num_shards
				a = a2a_shard_pop(&s.shards[peer])
				if a != nil do break
			}
		}
		if a == nil {
			intrinsics.cpu_relax()
			continue
		}

		processed := 0
		retired_tokens := 0
		for processed < 64 {
			tok := a2a_pop_detached(a)
			if tok == nil do break

			res := simulate_cpu_work(tok.payload ~ a.state)
			a.state ~= res
			processed += 1

			if tok.hops_left > 1 {
				tok.hops_left -= 1
				tok.payload = res
				target_id := (a.id * 31 + u32(res)) & SWARM_MASK
				target := &s.actors[target_id]
				a2a_push(target, tok)
				if intrinsics.atomic_exchange_explicit(&target.scheduled, 1, .Acq_Rel) == 0 {
					a2a_shard_push(my_shard, target)
				}
			} else {
				retired_tokens += 1
			}
		}

		if retired_tokens > 0 {
			intrinsics.atomic_sub_explicit(&s.remaining_tokens, retired_tokens, .Release)
		}

		if intrinsics.atomic_load_explicit(&a.head, .Acquire) != a.tail {
			a2a_shard_push(my_shard, a)
		} else {
			intrinsics.atomic_store_explicit(&a.scheduled, 0, .Seq_Cst)
			if intrinsics.atomic_load_explicit(&a.head, .Seq_Cst) != a.tail {
				if intrinsics.atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0 {
					a2a_shard_push(my_shard, a)
				}
			}
		}
	}
}

bench_pony_mn_a2a :: proc(total_hops: int, num_workers: int) -> (time.Duration, i64) {
	sched := new(A2A_Scheduler)
	defer free(sched)
	sched.num_shards = num_workers
	for w := 0; w < num_workers; w += 1 {
		a2a_shard_init(&sched.shards[w])
	}
	sched.remaining_tokens = A2A_INIT_TOKENS

	actors := make([]A2A_Actor, NUM_SWARM_ACTORS)
	defer delete(actors)
	for i := 0; i < NUM_SWARM_ACTORS; i += 1 {
		a2a_actor_init(&actors[i], u32(i))
	}
	sched.actors = actors

	tokens := make([]A2A_Token, A2A_INIT_TOKENS)
	defer delete(tokens)
	base_hops := total_hops / A2A_INIT_TOKENS
	extra     := total_hops % A2A_INIT_TOKENS

	for i := 0; i < A2A_INIT_TOKENS; i += 1 {
		tokens[i].payload   = u64(i + 1)
		tokens[i].hops_left = u32(base_hops + (1 if i < extra else 0))
		target := &actors[u32(i) & SWARM_MASK]
		a2a_push(target, &tokens[i])
		if intrinsics.atomic_exchange_explicit(&target.scheduled, 1, .Acq_Rel) == 0 {
			a2a_shard_push(&sched.shards[i % num_workers], target)
		}
	}

	workers := make([]^thread.Thread, num_workers)
	wctxs   := make([]A2A_Worker_Ctx, num_workers)
	defer delete(workers)
	defer delete(wctxs)

	start := time.tick_now()
	for w := 0; w < num_workers; w += 1 {
		wctxs[w] = A2A_Worker_Ctx{sched = sched, wid = w}
		workers[w] = thread.create(a2a_worker_proc)
		workers[w].data = &wctxs[w]
		thread.start(workers[w])
	}
	for w := 0; w < num_workers; w += 1 {
		thread.join(workers[w])
		thread.destroy(workers[w])
	}
	elapsed := time.tick_since(start)

	checksum: u64 = 0
	for i := 0; i < NUM_SWARM_ACTORS; i += 1 {
		checksum ~= actors[i].state
	}
	return elapsed, i64(checksum & 0x7FFFFFFFFFFFFFFF)
}

// =============================================================================
//Reusable `package actor` Benchmark Adapters (Workloads 1, 4, and 5)
// =============================================================================

Lib_FanIn_State :: struct {
	score: i64,
}

lib_fanin_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Lib_FanIn_State, i64), msg: ^i64) {
	self.state.score += msg^
}

Lib_FanIn_Prod_Ctx :: struct {
	sched:     ^actor.Scheduler,
	pool:      ^actor.Envelope_Pool(i64),
	target:    ^actor.Actor(Lib_FanIn_State, i64),
	count:     int,
	use_chain: bool,
	wg:        ^sync.Wait_Group,
}

lib_fanin_prod_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Lib_FanIn_Prod_Ctx)t.data
	if !ctx.use_chain {
		for i := 0; i < ctx.count; i += 1 {
			actor.send_from_pool(ctx.sched, ctx.pool, ctx.target, 1, 0)
		}
	} else {
		batch: [CHAIN_BATCH_LEN]i64
		for i := 0; i < CHAIN_BATCH_LEN; i += 1 {
			batch[i] = 1
		}
		remaining := ctx.count
		for remaining > 0 {
			k := min(CHAIN_BATCH_LEN, remaining)
			actor.send_batch_from_pool(ctx.sched, ctx.pool, ctx.target, batch[:k], 0)
			remaining -= k
		}
	}
	sync.wait_group_done(ctx.wg)
}

bench_library_fan_in :: proc(total_msgs: int, num_producers: int, use_chain: bool = false) -> (time.Duration, i64) {
	sched: actor.Scheduler
	actor.scheduler_init(&sched, {
		num_workers    = 1,
		num_shards     = 1,
		batch_quota    = PONY_BATCH_QUOTA,
		enable_parking = false,
	})
	sched.target_messages = i64(total_msgs)

	pools := make([]actor.Envelope_Pool(i64), num_producers)
	for p := 0; p < num_producers; p += 1 {
		actor.pool_init(&pools[p], u16(p), RING_SLAB_SIZE)
	}
	defer {
		for p := 0; p < num_producers; p += 1 {
			actor.pool_destroy(&pools[p])
		}
		delete(pools)
	}

	target := actor.spawn(Lib_FanIn_State{}, lib_fanin_receive, pools)

	var_wg: sync.Wait_Group
	sync.wait_group_add(&var_wg, num_producers)

	producers := make([]^thread.Thread, num_producers)
	pctxs     := make([]Lib_FanIn_Prod_Ctx, num_producers)
	defer delete(producers)
	defer delete(pctxs)
	defer actor.scheduler_stop_and_destroy(&sched)

	msgs_per_prod := total_msgs / num_producers
	start := time.tick_now()
	actor.scheduler_start(&sched)

	for p := 0; p < num_producers; p += 1 {
		pctxs[p] = Lib_FanIn_Prod_Ctx{
			sched     = &sched,
			pool      = &pools[p],
			target    = target,
			count     = msgs_per_prod,
			use_chain = use_chain,
			wg        = &var_wg,
		}
		producers[p] = thread.create(lib_fanin_prod_proc)
		producers[p].data = &pctxs[p]
		thread.start(producers[p])
	}

	sync.wait_group_wait(&var_wg)
	actor.scheduler_wait_messages(&sched, i64(total_msgs))
	elapsed := time.tick_since(start)

	for p := 0; p < num_producers; p += 1 {
		thread.destroy(producers[p])
	}
	return elapsed, target.state.score
}

Lib_Swarm_Prod_Ctx :: struct {
	sched:  ^actor.Scheduler,
	pool:   ^actor.Envelope_Pool(i64),
	actors: []actor.Actor(Lib_FanIn_State, i64),
	count:  int,
	seed:   u32,
	wg:     ^sync.Wait_Group,
}

lib_swarm_prod_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Lib_Swarm_Prod_Ctx)t.data
	for i := 0; i < ctx.count; i += 1 {
		target := &ctx.actors[(ctx.seed + u32(i)) & SWARM_MASK]
		actor.send_from_pool(ctx.sched, ctx.pool, target, 1)
	}
	sync.wait_group_done(ctx.wg)
}

bench_library_swarm :: proc(total_msgs: int, num_producers: int, num_actors: int) -> (time.Duration, i64) {
	sched: actor.Scheduler
	actor.scheduler_init(&sched, {
		num_workers    = NUM_WORKERS,
		num_shards     = num_producers,
		batch_quota    = PONY_BATCH_QUOTA,
		enable_parking = false,
	})
	sched.target_messages = i64(total_msgs)

	pools := make([]actor.Envelope_Pool(i64), num_producers)
	for p := 0; p < num_producers; p += 1 {
		actor.pool_init(&pools[p], u16(p), RING_SLAB_SIZE)
	}
	defer {
		for p := 0; p < num_producers; p += 1 {
			actor.pool_destroy(&pools[p])
		}
		delete(pools)
	}

	actors := make([]actor.Actor(Lib_FanIn_State, i64), num_actors)
	defer delete(actors)
	for i := 0; i < num_actors; i += 1 {
		actor.actor_init(&actors[i], Lib_FanIn_State{}, lib_fanin_receive, pools, u32(i))
	}

	var_wg: sync.Wait_Group
	sync.wait_group_add(&var_wg, num_producers)

	producers := make([]^thread.Thread, num_producers)
	pctxs     := make([]Lib_Swarm_Prod_Ctx, num_producers)
	defer delete(producers)
	defer delete(pctxs)
	defer actor.scheduler_stop_and_destroy(&sched)

	msgs_per_prod := total_msgs / num_producers
	start := time.tick_now()
	actor.scheduler_start(&sched)

	for p := 0; p < num_producers; p += 1 {
		pctxs[p] = Lib_Swarm_Prod_Ctx{
			sched  = &sched,
			pool   = &pools[p],
			actors = actors,
			count  = msgs_per_prod,
			seed   = u32(p * msgs_per_prod),
			wg     = &var_wg,
		}
		producers[p] = thread.create(lib_swarm_prod_proc)
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
	for i := 0; i < num_actors; i += 1 {
		total_score += actors[i].state.score
	}
	return elapsed, total_score
}

Lib_A2A_Token_Msg :: struct {
	payload:   u64,
	hops_left: u32,
}

Lib_A2A_State :: struct {
	hash_state: u64,
	peers:      []actor.Actor(Lib_A2A_State, Lib_A2A_Token_Msg),
}

lib_a2a_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Lib_A2A_State, Lib_A2A_Token_Msg), msg: ^Lib_A2A_Token_Msg) {
	res := simulate_cpu_work(msg.payload ~ self.state.hash_state)
	self.state.hash_state ~= res

	if msg.hops_left > 1 {
		target_id := (self.id * 31 + u32(res)) & SWARM_MASK
		target := &self.state.peers[target_id]
		actor.forward(ctx, target, Lib_A2A_Token_Msg{
			payload   = res,
			hops_left = msg.hops_left - 1,
		})
	} else {
		actor.complete_token(ctx, 1)
	}
}

bench_library_a2a :: proc(total_hops: int, num_workers: int) -> (time.Duration, i64) {
	sched: actor.Scheduler
	actor.scheduler_init(&sched, {
		num_workers    = num_workers,
		num_shards     = num_workers,
		batch_quota    = 64,
		enable_parking = false,
	})

	actors := make([]actor.Actor(Lib_A2A_State, Lib_A2A_Token_Msg), NUM_SWARM_ACTORS)
	defer delete(actors)
	for i := 0; i < NUM_SWARM_ACTORS; i += 1 {
		actor.actor_init(
			&actors[i],
			Lib_A2A_State{hash_state = 0, peers = actors},
			lib_a2a_receive,
			nil,
			u32(i),
		)
	}

	tokens := make([]actor.Envelope(Lib_A2A_Token_Msg), A2A_INIT_TOKENS)
	defer delete(tokens)
	defer actor.scheduler_stop_and_destroy(&sched)

	base_hops := total_hops / A2A_INIT_TOKENS
	extra     := total_hops % A2A_INIT_TOKENS
	sched.pending_tokens = A2A_INIT_TOKENS

	for i := 0; i < A2A_INIT_TOKENS; i += 1 {
		tokens[i].flags = actor.ENVELOPE_FLAG_STATIC
		tokens[i].msg = Lib_A2A_Token_Msg{
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
	return elapsed, i64(checksum & 0x7FFFFFFFFFFFFFFF)
}

// =============================================================================
// IMPLEMENTATION 4: Official Pony Runtime (`libponyrt` 0.77.2) via Odin C FFI
// Enabled with `-define:USE_LIBPONYRT=true`
// =============================================================================

USE_LIBPONYRT :: #config(USE_LIBPONYRT, false)

when USE_LIBPONYRT {
	foreign import ponyrt "../.tmp/ponyc_build/libponyrt.a"

	Pony_Msg :: struct {
		index: u32,
		id:    u32,
		next:  ^Pony_Msg,
	}

	Pony_Msg_Int :: struct {
		msg: Pony_Msg,
		i:   int,
	}

	Pony_Msg_Large :: struct {
		msg:  Pony_Msg,
		data: [64]i64,
	}

	Pony_MessageQ :: struct #align(64) {
		head:  ^Pony_Msg,
		tail:  ^Pony_Msg,
		_pad0: [48]u8,
	}

	Pony_Actor_Pad :: struct #align(64) {
		type: rawptr,
		pad:  [296]u8,
	}

	Pony_Type :: struct {
		id:                    u32,
		size:                  u32,
		field_count:           u32,
		field_offset:          u32,
		instance:              rawptr,
		name:                  cstring,
		get_behavior_name:     rawptr,
		trace:                 rawptr,
		dispatch:              proc "c" (ctx: rawptr, actor: rawptr, msg: ^Pony_Msg),
		final:                 rawptr,
		event_notify:          u32,
		might_reference_actor: bool,
		traits:                rawptr,
		fields:                rawptr,
		vtable:                rawptr,
	}

	@(default_calling_convention = "c")
	foreign ponyrt {
		pony_init                    :: proc(argc: c.int, argv: [^]cstring) -> c.int ---
		pony_start                   :: proc(exit_code: ^c.int, language_features: rawptr) -> bool ---
		pony_ctx                     :: proc() -> rawptr ---
		pony_create                  :: proc(ctx: rawptr, type: ^Pony_Type, orphaned: bool) -> rawptr ---
		pony_alloc_msg               :: proc(index: u32, id: u32) -> ^Pony_Msg ---
		pony_chain                   :: proc(prev: ^Pony_Msg, next: ^Pony_Msg) ---
		pony_sendv                   :: proc(ctx: rawptr, to: rawptr, first: ^Pony_Msg, last: ^Pony_Msg, has_app_msg: bool) ---
		ponyint_sendv_inject         :: proc(to: rawptr, msg: ^Pony_Msg) ---
		ponyint_sendi_inject         :: proc(to: rawptr, id: u32, i: int) ---
		ponyint_register_thread      :: proc() ---
		ponyint_unregister_thread    :: proc() ---
		ponyint_messageq_init        :: proc(q: ^Pony_MessageQ) ---
		ponyint_messageq_destroy     :: proc(q: ^Pony_MessageQ, maybe_non_empty: bool) ---
		ponyint_messageq_markempty   :: proc(q: ^Pony_MessageQ) -> bool ---
		ponyint_actor_messageq_push  :: proc(q: ^Pony_MessageQ, first: ^Pony_Msg, last: ^Pony_Msg) -> bool ---
		ponyint_actor_messageq_pop   :: proc(q: ^Pony_MessageQ) -> ^Pony_Msg ---
		ponyint_pool_index           :: proc(size: c.size_t) -> c.size_t ---
		ponyint_pool_drain           :: proc() ---
		ponyint_pool_thread_cleanup  :: proc() ---
		ponyint_asio_noisy_add       :: proc() -> u64 ---
		ponyint_asio_noisy_remove    :: proc() -> u64 ---
	}

	@(export, link_name = "Main_runtime_override_defaults_oo")
	main_runtime_override_defaults_oo :: proc "c" (opt: rawptr) {}

	foreign import libc "system:c"
	@(default_calling_convention = "c")
	foreign libc {
		fork    :: proc() -> c.int ---
		execv   :: proc(path: cstring, argv: [^]cstring) -> c.int ---
		waitpid :: proc(pid: c.int, status: ^c.int, options: c.int) -> c.int ---
		_exit   :: proc(status: c.int) -> ! ---
	}

	// -------------------------------------------------------------------------
	// 4a / 4b: Workload 1 — `libponyrt` MPSC Fan-In (Single & Chain=32)
	// -------------------------------------------------------------------------
	PonyRT_FanIn_Prod_Ctx :: struct {
		q:         ^Pony_MessageQ,
		count:     int,
		use_chain: bool,
		wg:        ^sync.Wait_Group,
	}

	ponyrt_fanin_producer_proc :: proc(t: ^thread.Thread) {
		ctx := cast(^PonyRT_FanIn_Prod_Ctx)t.data
		idx := u32(ponyint_pool_index(size_of(Pony_Msg_Int)))
		if !ctx.use_chain {
			for i := 0; i < ctx.count; i += 1 {
				m := cast(^Pony_Msg_Int)pony_alloc_msg(idx, 0)
				m.i = 1
				ponyint_actor_messageq_push(ctx.q, &m.msg, &m.msg)
			}
		} else {
			rem := ctx.count
			for rem > 0 {
				k := min(CHAIN_BATCH_LEN, rem)
				first := cast(^Pony_Msg_Int)pony_alloc_msg(idx, 0)
				first.i = 1
				prev := &first.msg
				for j := 1; j < k; j += 1 {
					cur := cast(^Pony_Msg_Int)pony_alloc_msg(idx, 0)
					cur.i = 1
					pony_chain(prev, &cur.msg)
					prev = &cur.msg
				}
				ponyint_actor_messageq_push(ctx.q, &first.msg, prev)
				rem -= k
			}
		}
		ponyint_pool_thread_cleanup()
		sync.wait_group_done(ctx.wg)
	}

	bench_libponyrt_fan_in :: proc(total_msgs: int, num_producers: int, use_chain: bool) -> (time.Duration, i64) {
		q := new(Pony_MessageQ)
		defer free(q)
		ponyint_messageq_init(q)

		var_wg: sync.Wait_Group
		sync.wait_group_add(&var_wg, num_producers)

		msgs_per_prod := total_msgs / num_producers
		producers := make([]^thread.Thread, num_producers)
		ctxs      := make([]PonyRT_FanIn_Prod_Ctx, num_producers)
		defer delete(producers)
		defer delete(ctxs)

		start := time.tick_now()
		for p := 0; p < num_producers; p += 1 {
			ctxs[p] = PonyRT_FanIn_Prod_Ctx{
				q         = q,
				count     = msgs_per_prod,
				use_chain = use_chain,
				wg        = &var_wg,
			}
			producers[p] = thread.create(ponyrt_fanin_producer_proc)
			producers[p].data = &ctxs[p]
			thread.start(producers[p])
		}

		received := 0
		score: i64 = 0
		for received < total_msgs {
			batch := 0
			raw_head := uintptr(intrinsics.atomic_load_explicit(&q.head, .Acquire))
			head := (^Pony_Msg)(raw_head & ~uintptr(1))
			for batch < PONY_BATCH_QUOTA {
				msg := ponyint_actor_messageq_pop(q)
				if msg == nil do break
				m := cast(^Pony_Msg_Int)msg
				score += i64(m.i)
				received += 1
				batch += 1
				if msg == head do break
			}
			if batch == 0 {
				ponyint_messageq_markempty(q)
				intrinsics.cpu_relax()
			}
		}

		sync.wait_group_wait(&var_wg)
		elapsed := time.tick_since(start)

		for p := 0; p < num_producers; p += 1 {
			thread.destroy(producers[p])
		}
		ponyint_messageq_markempty(q)
		ponyint_messageq_destroy(q, true)
		ponyint_pool_drain()
		return elapsed, score
	}

	// -------------------------------------------------------------------------
	// 4: Workload 2 — `libponyrt` Cross-Thread 1-to-1 Ping-Pong
	// -------------------------------------------------------------------------
	PonyRT_PingPong_Ctx :: struct #align(64) {
		q_a:   Pony_MessageQ,
		q_b:   Pony_MessageQ,
		trips: int,
	}

	ponyrt_pong_worker_proc :: proc(t: ^thread.Thread) {
		ctx := cast(^PonyRT_PingPong_Ctx)t.data
		idx := u32(ponyint_pool_index(size_of(Pony_Msg_Int)))
		for i := 0; i < ctx.trips; i += 1 {
			msg: ^Pony_Msg
			for {
				msg = ponyint_actor_messageq_pop(&ctx.q_a)
				if msg != nil do break
				intrinsics.cpu_relax()
			}
			v := (cast(^Pony_Msg_Int)msg).i
			ponyint_messageq_markempty(&ctx.q_a)

			out_m := cast(^Pony_Msg_Int)pony_alloc_msg(idx, 0)
			out_m.i = v + 1
			ponyint_actor_messageq_push(&ctx.q_b, &out_m.msg, &out_m.msg)
		}
		ponyint_pool_thread_cleanup()
	}

	bench_libponyrt_ping_pong :: proc(trips: int) -> (time.Duration, i64) {
		ctx := new(PonyRT_PingPong_Ctx)
		defer free(ctx)
		ponyint_messageq_init(&ctx.q_a)
		ponyint_messageq_init(&ctx.q_b)
		ctx.trips = trips

		pong_thread := thread.create(ponyrt_pong_worker_proc)
		pong_thread.data = ctx
		defer thread.destroy(pong_thread)

		idx := u32(ponyint_pool_index(size_of(Pony_Msg_Int)))
		val := 0

		start := time.tick_now()
		thread.start(pong_thread)

		for i := 0; i < trips; i += 1 {
			m := cast(^Pony_Msg_Int)pony_alloc_msg(idx, 0)
			m.i = val
			ponyint_actor_messageq_push(&ctx.q_a, &m.msg, &m.msg)

			for {
				rep := ponyint_actor_messageq_pop(&ctx.q_b)
				if rep != nil {
					val = (cast(^Pony_Msg_Int)rep).i
					ponyint_messageq_markempty(&ctx.q_b)
					break
				}
				intrinsics.cpu_relax()
			}
		}
		thread.join(pong_thread)
		elapsed := time.tick_since(start)

		ponyint_messageq_destroy(&ctx.q_a, true)
		ponyint_messageq_destroy(&ctx.q_b, true)
		ponyint_pool_drain()
		return elapsed, i64(val)
	}

	// -------------------------------------------------------------------------
	// 4c: Workload 3 — `libponyrt` 512-Byte Large Payload Pool
	// -------------------------------------------------------------------------
	ponyrt_large_producer_proc :: proc(t: ^thread.Thread) {
		ctx := cast(^PonyRT_FanIn_Prod_Ctx)t.data
		idx := u32(ponyint_pool_index(size_of(Pony_Msg_Large)))
		for i := 0; i < ctx.count; i += 1 {
			m := cast(^Pony_Msg_Large)pony_alloc_msg(idx, 0)
			m.data[0]  = 1
			m.data[63] = 1
			ponyint_actor_messageq_push(ctx.q, &m.msg, &m.msg)
		}
		ponyint_pool_thread_cleanup()
		sync.wait_group_done(ctx.wg)
	}

	bench_libponyrt_large :: proc(total_msgs: int, num_producers: int) -> (time.Duration, i64) {
		q := new(Pony_MessageQ)
		defer free(q)
		ponyint_messageq_init(q)

		var_wg: sync.Wait_Group
		sync.wait_group_add(&var_wg, num_producers)

		msgs_per_prod := total_msgs / num_producers
		producers := make([]^thread.Thread, num_producers)
		ctxs      := make([]PonyRT_FanIn_Prod_Ctx, num_producers)
		defer delete(producers)
		defer delete(ctxs)

		start := time.tick_now()
		for p := 0; p < num_producers; p += 1 {
			ctxs[p] = PonyRT_FanIn_Prod_Ctx{
				q     = q,
				count = msgs_per_prod,
				wg    = &var_wg,
			}
			producers[p] = thread.create(ponyrt_large_producer_proc)
			producers[p].data = &ctxs[p]
			thread.start(producers[p])
		}

		received := 0
		score: i64 = 0
		for received < total_msgs {
			batch := 0
			raw_head := uintptr(intrinsics.atomic_load_explicit(&q.head, .Acquire))
			head := (^Pony_Msg)(raw_head & ~uintptr(1))
			for batch < PONY_BATCH_QUOTA {
				msg := ponyint_actor_messageq_pop(q)
				if msg == nil do break
				m := cast(^Pony_Msg_Large)msg
				score += m.data[0] + m.data[63]
				received += 1
				batch += 1
				if msg == head do break
			}
			if batch == 0 {
				ponyint_messageq_markempty(q)
				intrinsics.cpu_relax()
			}
		}

		sync.wait_group_wait(&var_wg)
		elapsed := time.tick_since(start)

		for p := 0; p < num_producers; p += 1 {
			thread.destroy(producers[p])
		}
		ponyint_messageq_markempty(q)
		ponyint_messageq_destroy(q, true)
		ponyint_pool_drain()
		return elapsed, score
	}

	// -------------------------------------------------------------------------
	// 4d & 4e: Full `libponyrt` M:N Runtime (Workloads 4 & 5) via Isolated Exec
	// (`pony_init` / `pony_start` can only be called once per OS process, and
	// `libponyrt`'s global pool arena state must start clean before any thread
	// allocates from `pony_alloc_msg`).
	// -------------------------------------------------------------------------
	PonyRT_Swarm_Actor :: struct #align(64) {
		pad:   Pony_Actor_Pad,
		score: i64,
	}

	g_ponyrt_swarm_rem:   int
	g_ponyrt_finish_tick: time.Tick

	ponyrt_swarm_dispatch :: proc "c" (ctx: rawptr, actor_ptr: rawptr, m: ^Pony_Msg) {
		self := cast(^PonyRT_Swarm_Actor)actor_ptr
		msg  := cast(^Pony_Msg_Int)m
		self.score += i64(msg.i)
		if intrinsics.atomic_sub_explicit(&g_ponyrt_swarm_rem, 1, .Acq_Rel) == 1 {
			g_ponyrt_finish_tick = time.tick_now()
			ponyint_asio_noisy_remove()
		}
	}

	g_ponyrt_swarm_type := Pony_Type{
		id           = 0,
		size         = size_of(PonyRT_Swarm_Actor),
		name         = "SwarmActor",
		dispatch     = ponyrt_swarm_dispatch,
		event_notify = 0xFFFFFFFF,
	}

	PonyRT_Swarm_Prod_Ctx :: struct {
		actors: []^PonyRT_Swarm_Actor,
		count:  int,
		seed:   u32,
		wg:     ^sync.Wait_Group,
	}

	ponyrt_swarm_prod_proc :: proc(t: ^thread.Thread) {
		ctx := cast(^PonyRT_Swarm_Prod_Ctx)t.data
		ponyint_register_thread()
		for i := 0; i < ctx.count; i += 1 {
			target := ctx.actors[(ctx.seed + u32(i)) & SWARM_MASK]
			ponyint_sendi_inject(target, 0, 1)
		}
		ponyint_unregister_thread()
		sync.wait_group_done(ctx.wg)
	}

	run_child_ponyrt_swarm :: proc(total_msgs: int, num_producers: int, num_actors: int) {
		argv := [?]cstring{"bench", "--ponymaxthreads=2", "--ponynoscale", "--ponynoblock", nil}
		pony_init(4, raw_data(argv[:]))
		ctx := pony_ctx()

		actors := make([]^PonyRT_Swarm_Actor, num_actors)
		for i := 0; i < num_actors; i += 1 {
			a := cast(^PonyRT_Swarm_Actor)pony_create(ctx, &g_ponyrt_swarm_type, false)
			// Keep rc > 0 (`offsetof(pony_actor_t, gc.rc) == 224`) so actors aren't fast-reaped between waves
			(^c.size_t)(uintptr(a) + 224)^ = 1
			a.score = 0
			actors[i] = a
		}

		g_ponyrt_swarm_rem = total_msgs
		ponyint_asio_noisy_add()
		var_wg: sync.Wait_Group
		sync.wait_group_add(&var_wg, num_producers)

		producers := make([]^thread.Thread, num_producers)
		pctxs     := make([]PonyRT_Swarm_Prod_Ctx, num_producers)
		msgs_per_prod := total_msgs / num_producers

		start := time.tick_now()
		for p := 0; p < num_producers; p += 1 {
			pctxs[p] = PonyRT_Swarm_Prod_Ctx{
				actors = actors,
				count  = msgs_per_prod,
				seed   = u32(p * msgs_per_prod),
				wg     = &var_wg,
			}
			producers[p] = thread.create(ponyrt_swarm_prod_proc)
			producers[p].data = &pctxs[p]
			thread.start(producers[p])
		}

		exit_code: c.int = 0
		pony_start(&exit_code, nil)
		sync.wait_group_wait(&var_wg)

		elapsed := time.tick_diff(start, g_ponyrt_finish_tick)
		total_score: i64 = 0
		for i := 0; i < num_actors; i += 1 {
			total_score += actors[i].score
		}
		print_row("4d. Official Pony (`libponyrt` 0.77.2 M:N Swarm)", total_msgs, elapsed, total_score)
		_exit(0)
	}

	PonyRT_A2A_Msg :: struct {
		msg:       Pony_Msg,
		payload:   u64,
		hops_left: u32,
	}

	PonyRT_A2A_Actor :: struct #align(64) {
		pad:   Pony_Actor_Pad,
		id:    u32,
		state: u64,
		peers: [^]^PonyRT_A2A_Actor,
	}

	g_ponyrt_a2a_rem:   int
	g_ponyrt_a2a_index: u32

	ponyrt_a2a_dispatch :: proc "c" (ctx: rawptr, actor_ptr: rawptr, m: ^Pony_Msg) {
		self := cast(^PonyRT_A2A_Actor)actor_ptr
		tok  := cast(^PonyRT_A2A_Msg)m

		res := simulate_cpu_work(tok.payload ~ self.state)
		self.state ~= res

		if tok.hops_left > 1 {
			target_id := (self.id * 31 + u32(res)) & SWARM_MASK
			target := self.peers[target_id]
			next_m := cast(^PonyRT_A2A_Msg)pony_alloc_msg(g_ponyrt_a2a_index, 0)
			next_m.payload   = res
			next_m.hops_left = tok.hops_left - 1
			pony_sendv(ctx, target, &next_m.msg, &next_m.msg, true)
		} else {
			if intrinsics.atomic_sub_explicit(&g_ponyrt_a2a_rem, 1, .Acq_Rel) == 1 {
				g_ponyrt_finish_tick = time.tick_now()
			}
		}
	}

	g_ponyrt_a2a_type := Pony_Type{
		id           = 1,
		size         = size_of(PonyRT_A2A_Actor),
		name         = "A2AActor",
		dispatch     = ponyrt_a2a_dispatch,
		event_notify = 0xFFFFFFFF,
	}

	run_child_ponyrt_a2a :: proc(total_hops: int, num_threads: int) {
		arg_buf: [32]u8
		arg_str := fmt.bprintf(arg_buf[:], "--ponymaxthreads=%d\x00", num_threads)
		argv := [?]cstring{"bench", cstring(raw_data(arg_str)), "--ponynoscale", "--ponynoblock", nil}
		pony_init(4, raw_data(argv[:]))
		ctx := pony_ctx()

		g_ponyrt_a2a_index = u32(ponyint_pool_index(size_of(PonyRT_A2A_Msg)))
		actors := make([]^PonyRT_A2A_Actor, NUM_SWARM_ACTORS)
		for i := 0; i < NUM_SWARM_ACTORS; i += 1 {
			a := cast(^PonyRT_A2A_Actor)pony_create(ctx, &g_ponyrt_a2a_type, false)
			// `offsetof(pony_actor_t, gc.rc) == 224`
			(^c.size_t)(uintptr(a) + 224)^ = 1
			a.id    = u32(i)
			a.state = 0
			a.peers = raw_data(actors)
			actors[i] = a
		}

		g_ponyrt_a2a_rem = A2A_INIT_TOKENS
		base_hops := total_hops / A2A_INIT_TOKENS
		extra     := total_hops % A2A_INIT_TOKENS

		for i := 0; i < A2A_INIT_TOKENS; i += 1 {
			m := cast(^PonyRT_A2A_Msg)pony_alloc_msg(g_ponyrt_a2a_index, 0)
			m.payload   = u64(i + 1)
			m.hops_left = u32(base_hops + (1 if i < extra else 0))
			ponyint_sendv_inject(actors[u32(i) & SWARM_MASK], &m.msg)
		}

		start := time.tick_now()
		exit_code: c.int = 0
		pony_start(&exit_code, nil)
		elapsed := time.tick_diff(start, g_ponyrt_finish_tick)

		checksum: u64 = 0
		for i := 0; i < NUM_SWARM_ACTORS; i += 1 {
			checksum ~= actors[i].state
		}
		label_buf: [64]u8
		label := fmt.bprintf(
			label_buf[:],
			"4e. Official Pony (`libponyrt` 0.77.2, %d %s)",
			num_threads,
			"Thread" if num_threads == 1 else "Threads",
		)
		print_row(label, total_hops, elapsed, i64(checksum & 0x7FFFFFFFFFFFFFFF))
		_exit(0)
	}

	spawn_self_mode :: proc(mode_arg: cstring) {
		pid := fork()
		if pid == 0 {
			exe: cstring = "/proc/self/exe"
			argv := [?]cstring{exe, mode_arg, nil}
			execv(exe, raw_data(argv[:]))
			_exit(1)
		}
		status: c.int
		waitpid(pid, &status, 0)
	}
}

// =============================================================================
// Main Benchmark Runner & hugi Packet Support
// =============================================================================

Bench_CLI_Config :: struct {
	runs:            int,
	warmup:          int,
	groups_filter:   string,
	name_filter:     string,
	lang:            string,
	commit:          string,
	run_id:          string,
	packet_out_path: string,
	quiet:           bool,
}

_bench_cli_destroy :: proc(cfg: ^Bench_CLI_Config, allocator := context.allocator) {
	delete(cfg.groups_filter, allocator)
	delete(cfg.name_filter, allocator)
	delete(cfg.lang, allocator)
	delete(cfg.commit, allocator)
	delete(cfg.run_id, allocator)
	delete(cfg.packet_out_path, allocator)
	cfg^ = {}
}

_replace_cli_str :: proc(dst: ^string, val: string, allocator := context.allocator) {
	delete(dst^, allocator)
	dst^ = strings.clone(val, allocator)
}

_print_bench_usage :: proc() {
	fmt.println("Usage: actor_benchmark [options]")
	fmt.println("  --packet-out PATH  Write hugi Run_Packet (.hpk) to PATH")
	fmt.println("  --runs N           Measured runs per benchmark (default: 3 with --packet-out, 1 otherwise)")
	fmt.println("  --warmup N         Warmup runs per benchmark (default: 1 with --packet-out, 0 otherwise)")
	fmt.println("  --groups LIST      Comma/pipe-separated workload groups")
	fmt.println("                     (workload_1_fan_in,workload_2_ping_pong,workload_3_large_payload,workload_4_swarm,workload_5_a2a)")
	fmt.println("  --filter SUBSTR    Run only benchmarks matching substring (supports ',' or '|')")
	fmt.println("  --commit SHA       Override git commit SHA")
	fmt.println("  --lang NAME        Language tag in packet (default: odin)")
	fmt.println("  --run-id ID        Override run ID")
	fmt.println("  --quiet            Suppress terminal table output")
}

_parse_bench_cli :: proc(args: []string, allocator := context.allocator) -> (cfg: Bench_CLI_Config, ok: bool, show_help: bool) {
	cfg = Bench_CLI_Config{
		lang = strings.clone("odin", allocator),
	}
	runs_explicit := false
	warmup_explicit := false

	i := 0
	for i < len(args) {
		arg := args[i]
		i += 1
		if arg == "-h" || arg == "--help" {
			return cfg, true, true
		}
		if arg == "--quiet" || arg == "-quiet" {
			cfg.quiet = true
			continue
		}
		if !strings.has_prefix(arg, "-") {
			_bench_cli_destroy(&cfg, allocator)
			return {}, false, false
		}
		trimmed := strings.trim_left(arg, "-")
		key := trimmed
		val := ""
		has_inline := false
		if sep := strings.index_any(trimmed, "=:"); sep >= 0 {
			key = trimmed[:sep]
			val = trimmed[sep + 1:]
			has_inline = true
		}
		switch key {
		case "runs", "warmup", "groups", "group", "filter", "f", "lang", "commit", "run-id", "packet-out":
		case:
			_bench_cli_destroy(&cfg, allocator)
			return {}, false, false
		}
		if !has_inline {
			if i >= len(args) {
				_bench_cli_destroy(&cfg, allocator)
				return {}, false, false
			}
			val = args[i]
			i += 1
		}
		switch key {
		case "runs":
			n, p_ok := strconv.parse_int(val)
			if !p_ok || n < 1 {
				_bench_cli_destroy(&cfg, allocator)
				return {}, false, false
			}
			cfg.runs = n
			runs_explicit = true
		case "warmup":
			n, p_ok := strconv.parse_int(val)
			if !p_ok || n < 0 {
				_bench_cli_destroy(&cfg, allocator)
				return {}, false, false
			}
			cfg.warmup = n
			warmup_explicit = true
		case "groups", "group":
			_replace_cli_str(&cfg.groups_filter, val, allocator)
		case "filter", "f":
			_replace_cli_str(&cfg.name_filter, val, allocator)
		case "lang":
			lower := strings.to_lower(val, context.temp_allocator)
			_replace_cli_str(&cfg.lang, lower, allocator)
		case "commit":
			_replace_cli_str(&cfg.commit, val, allocator)
		case "run-id":
			_replace_cli_str(&cfg.run_id, val, allocator)
		case "packet-out":
			_replace_cli_str(&cfg.packet_out_path, val, allocator)
		}
	}

	if !runs_explicit {
		cfg.runs = 3 if cfg.packet_out_path != "" else 1
	}
	if !warmup_explicit {
		cfg.warmup = 1 if cfg.packet_out_path != "" else 0
	}
	if cfg.commit == "" {
		_replace_cli_str(&cfg.commit, hugi.resolve_git_commit(".", context.temp_allocator), allocator)
	}
	if cfg.run_id == "" {
		_replace_cli_str(&cfg.run_id, hugi.default_run_id(context.temp_allocator), allocator)
	}
	return cfg, true, false
}

Workload_Section :: struct {
	group:          string,
	title:          string,
	show_col_hdr:   bool,
	header_printed: bool,
}

_workload_end :: proc(cfg: ^Bench_CLI_Config, sec: ^Workload_Section) {
	if sec.header_printed && !cfg.quiet {
		fmt.println("-----------------------------------------------------------------------------------------------------------")
	}
}

Bench_Run_Ctx :: struct {
	cfg:       ^Bench_CLI_Config,
	records:   [dynamic]hugi.Record,
	next_rank: int,
}

Bench_Loop :: struct {
	ctx:              ^Bench_Run_Ctx,
	rank:             int,
	group:            string,
	bench_name:       string,
	full_name:        string,
	display_label:    string,
	msgs:             int,
	bytes_per_op:     u64,
	threads:          int,
	has_threads:      bool,
	active:           bool,
	trial:            int,
	total_trials:     int,
	warmup:           int,
	last_score:       i64,
	samples_batch_ns: [dynamic]u64,
}

_matches_bench_filter :: proc(cfg: ^Bench_CLI_Config, group, full_name, display_label: string) -> bool {
	if !hugi.should_run_group(cfg.groups_filter, group) {
		return false
	}
	if cfg.name_filter == "" {
		return true
	}
	return hugi.matches_name_filter(cfg.name_filter, full_name) || hugi.matches_name_filter(cfg.name_filter, display_label)
}

bench_begin :: proc(
	bctx:          ^Bench_Run_Ctx,
	sec:           ^Workload_Section,
	bench_name:    string,
	display_label: string,
	msgs:          int,
	bytes_per_op:  u64 = 0,
	threads:       int = 0,
	has_threads:   bool = false,
) -> Bench_Loop {
	bctx.next_rank += 1
	rank := bctx.next_rank
	full_name := fmt.tprintf("%s/%s", sec.group, bench_name)
	if !_matches_bench_filter(bctx.cfg, sec.group, full_name, display_label) {
		return {}
	}

	if !sec.header_printed && !bctx.cfg.quiet {
		if sec.group != "workload_1_fan_in" {
			fmt.println("")
		}
		fmt.println("===========================================================================================================")
		fmt.println(sec.title)
		fmt.println("===========================================================================================================")
		if sec.show_col_hdr {
			fmt.println("| Implementation                                   | Total Time  | Throughput    | Latency/Msg  | Checksum |")
			fmt.println("|--------------------------------------------------|-------------|---------------|--------------|----------|")
		}
		sec.header_printed = true
	}

	return Bench_Loop{
		ctx              = bctx,
		rank             = rank,
		group            = sec.group,
		bench_name       = strings.clone(bench_name),
		full_name        = strings.clone(full_name),
		display_label    = strings.clone(display_label),
		msgs             = msgs,
		bytes_per_op     = bytes_per_op,
		threads          = threads,
		has_threads      = has_threads,
		active           = true,
		trial            = 0,
		total_trials     = bctx.cfg.warmup + bctx.cfg.runs,
		warmup           = bctx.cfg.warmup,
		samples_batch_ns = make([dynamic]u64, 0, bctx.cfg.runs),
	}
}

bench_sample :: #force_inline proc(b: ^Bench_Loop, elapsed: time.Duration, score: i64) {
	if !b.active {
		return
	}
	if b.trial >= b.warmup {
		ns := time.duration_nanoseconds(elapsed)
		append(&b.samples_batch_ns, u64(max(ns, 0)))
	}
	b.last_score = score
	b.trial += 1
}

_bench_finish :: proc(b: ^Bench_Loop) {
	defer {
		delete(b.bench_name)
		delete(b.full_name)
		delete(b.display_label)
		delete(b.samples_batch_ns)
		b.active = false
	}
	if len(b.samples_batch_ns) == 0 {
		return
	}

	rec := hugi.Record{
		rank             = b.rank,
		group_name       = strings.clone(b.group),
		name             = strings.clone(b.bench_name),
		full_name        = strings.clone(b.full_name),
		threads          = b.threads,
		has_threads      = b.has_threads,
		ops_per_sample   = u64(max(b.msgs, 1)),
		samples          = len(b.samples_batch_ns),
		warmup           = b.warmup,
		bytes_per_op     = b.bytes_per_op,
		error            = strings.clone(""),
		metrics          = make([dynamic]hugi.Metric),
		samples_batch_ns = make([dynamic]u64, len(b.samples_batch_ns)),
	}
	copy(rec.samples_batch_ns[:], b.samples_batch_ns[:])
	hugi.recompute_record_stats_from_samples(&rec)
	hugi.refresh_record_throughput(&rec, overwrite = true)

	if !b.ctx.cfg.quiet {
		med_total_ns := (i128(rec.p50_fs) * i128(max(b.msgs, 1)) + 500_000) / 1_000_000
		print_row(b.display_label, b.msgs, time.Duration(med_total_ns), b.last_score)
	}

	if b.ctx.cfg.packet_out_path != "" {
		append(&b.ctx.records, rec)
	} else {
		hugi.record_destroy(&rec)
	}
}

bench_next :: proc(b: ^Bench_Loop) -> bool {
	if !b.active {
		return false
	}
	if b.trial >= b.total_trials {
		_bench_finish(b)
		return false
	}
	return true
}

print_row :: proc(name: string, msgs: int, elapsed: time.Duration, score: i64) {
	sec := time.duration_seconds(elapsed)
	mops := (f64(msgs) / sec) / 1_000_000.0
	ns_per_msg := (sec * 1_000_000_000.0) / f64(msgs)
	fmt.printfln(
		"| %-48s | %8.2f ms | %9.2f M/s | %8.1f ns/msg | %d |",
		name,
		sec * 1000.0,
		mops,
		ns_per_msg,
		score,
	)
}

main :: proc() {
	when USE_LIBPONYRT {
		if len(os.args) > 1 {
			mode := os.args[1]
			if strings.has_prefix(mode, "--child-pony-") {
				switch mode {
				case "--child-pony-w4":
					run_child_ponyrt_swarm(TOTAL_MESSAGES, NUM_PRODUCERS, NUM_SWARM_ACTORS)
				case "--child-pony-w5-1":
					run_child_ponyrt_a2a(A2A_TOTAL_HOPS, 1)
				case "--child-pony-w5-2":
					run_child_ponyrt_a2a(A2A_TOTAL_HOPS, 2)
				case "--child-pony-w5-4":
					run_child_ponyrt_a2a(A2A_TOTAL_HOPS, 4)
				case "--child-pony-w5-8":
					run_child_ponyrt_a2a(A2A_TOTAL_HOPS, 8)
				}
				return
			}
		}
	}

	args := os.args[1:] if len(os.args) > 1 else nil
	cfg, cli_ok, show_help := _parse_bench_cli(args)
	if !cli_ok {
		fmt.eprintln("actor_benchmark: invalid command-line arguments")
		_print_bench_usage()
		os.exit(1)
	}
	defer _bench_cli_destroy(&cfg)
	if show_help {
		_print_bench_usage()
		return
	}

	bctx := Bench_Run_Ctx{
		cfg     = &cfg,
		records = make([dynamic]hugi.Record),
	}
	defer {
		for &r in bctx.records {
			hugi.record_destroy(&r)
		}
		delete(bctx.records)
	}

	// Workload 1: MPSC Fan-In Contention
	w1 := Workload_Section{
		group        = "workload_1_fan_in",
		title        = fmt.tprintf("WORKLOAD 1: MPSC Fan-In Contention (%d messages, %d producers -> 1 actor)", TOTAL_MESSAGES, NUM_PRODUCERS),
		show_col_hdr = true,
	}
	for b := bench_begin(&bctx, &w1, "1_chan_fan_in", "1. Traditional (core:sync/chan + thread)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_chan_actor_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS)
		bench_sample(&b, d, s)
	}
	for b := bench_begin(&bctx, &w1, "2a_dod_single_thread", "2a. DOD Batch Array (Single-Thread)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_dod_single_thread(TOTAL_MESSAGES, NUM_SWARM_ACTORS)
		bench_sample(&b, d, s)
	}
	for b := bench_begin(&bctx, &w1, "2b_dod_multithread_tls", "2b. DOD Multi-Thread (4 TLS Buffers + Drain)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_dod_multithread_tls(TOTAL_MESSAGES, NUM_PRODUCERS, NUM_SWARM_ACTORS)
		bench_sample(&b, d, s)
	}
	for b := bench_begin(&bctx, &w1, "3b_self_pony_recycled_fan_in", "3b. Self-Impl Pony MPSC (Per-Msg 8K Slab)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_self_pony_recycled_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS)
		bench_sample(&b, d, s)
	}
	for b := bench_begin(&bctx, &w1, "3c_self_pony_batched_fan_in", "3c. Self-Impl Pony MPSC (Batched Watermark 8K)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_self_pony_batched_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS, false, false)
		bench_sample(&b, d, s)
	}
	for b := bench_begin(&bctx, &w1, "3d_self_pony_chain32_fan_in", "3d. Self-Impl Pony MPSC (Chain=32 Splice 8K)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_self_pony_batched_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS, true, false)
		bench_sample(&b, d, s)
	}
	for b := bench_begin(&bctx, &w1, "3e_sharded_spsc_fan_in", "3e. Sharded SPSC Ring Lanes (Zero RMW Atomics)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_sharded_spsc_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS)
		bench_sample(&b, d, s)
	}
	for b := bench_begin(&bctx, &w1, "3f_self_pony_parker_fan_in", "3f. Self-Impl Pony MPSC + Adaptive Futex Parker", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_self_pony_batched_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS, false, true)
		bench_sample(&b, d, s)
	}
	when USE_LIBPONYRT {
		for b := bench_begin(&bctx, &w1, "4a_libponyrt_single_fan_in", "4a. Official Pony (`libponyrt` 0.77.2 Single)", TOTAL_MESSAGES); bench_next(&b); {
			d, s := bench_libponyrt_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS, false)
			bench_sample(&b, d, s)
		}
		for b := bench_begin(&bctx, &w1, "4b_libponyrt_chain32_fan_in", "4b. Official Pony (`libponyrt` 0.77.2 Chain=32)", TOTAL_MESSAGES); bench_next(&b); {
			d, s := bench_libponyrt_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS, true)
			bench_sample(&b, d, s)
		}
	}
	for b := bench_begin(&bctx, &w1, "5a_actor_pool_single_fan_in", "5a. `package actor` (Envelope_Pool Single Send)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_library_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS, false)
		bench_sample(&b, d, s)
	}
	for b := bench_begin(&bctx, &w1, "5a_actor_pool_chain32_fan_in", "5a. `package actor` (Envelope_Pool Chain=32)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_library_fan_in(TOTAL_MESSAGES, NUM_PRODUCERS, true)
		bench_sample(&b, d, s)
	}
	_workload_end(&cfg, &w1)

	// Workload 2: Cross-Thread 1-to-1 Ping-Pong Latency
	w2 := Workload_Section{
		group = "workload_2_ping_pong",
		title = fmt.tprintf("WORKLOAD 2: Cross-Thread 1-to-1 Ping-Pong Latency (%d round-trips = %d hops)", PING_PONG_TRIPS, PING_PONG_TRIPS * 2),
	}
	for b := bench_begin(&bctx, &w2, "1_chan_ping_pong", "1. Traditional (core:sync/chan) Ping-Pong", PING_PONG_TRIPS * 2); bench_next(&b); {
		d, s := bench_chan_ping_pong(PING_PONG_TRIPS)
		bench_sample(&b, d, s)
	}
	for b := bench_begin(&bctx, &w2, "3_self_pony_ping_pong", "3. Self-Impl Pony MPSC Ping-Pong", PING_PONG_TRIPS * 2); bench_next(&b); {
		d, s := bench_self_pony_ping_pong(PING_PONG_TRIPS)
		bench_sample(&b, d, s)
	}
	when USE_LIBPONYRT {
		for b := bench_begin(&bctx, &w2, "4_libponyrt_ping_pong", "4. Official Pony (`libponyrt` 0.77.2) Ping-Pong", PING_PONG_TRIPS * 2); bench_next(&b); {
			d, s := bench_libponyrt_ping_pong(PING_PONG_TRIPS)
			bench_sample(&b, d, s)
		}
	}
	_workload_end(&cfg, &w2)

	// Workload 3: 512-Byte Large Payload Zero-Copy Crossover
	w3 := Workload_Section{
		group = "workload_3_large_payload",
		title = fmt.tprintf("WORKLOAD 3: 512-Byte Large Payload Zero-Copy Crossover (%d messages)", LARGE_MESSAGES),
	}
	for b := bench_begin(&bctx, &w3, "3c_pony_zerocopy_512b", "3c. Pony MPSC Zero-Copy `iso` Pointer (512B)", LARGE_MESSAGES, bytes_per_op = 512); bench_next(&b); {
		d, s := bench_large_pony_zerocopy(LARGE_MESSAGES, NUM_PRODUCERS)
		bench_sample(&b, d, s)
	}
	when USE_LIBPONYRT {
		for b := bench_begin(&bctx, &w3, "4c_libponyrt_512b_pool", "4c. Official Pony (`libponyrt` 0.77.2 512B Pool)", LARGE_MESSAGES, bytes_per_op = 512); bench_next(&b); {
			d, s := bench_libponyrt_large(LARGE_MESSAGES, NUM_PRODUCERS)
			bench_sample(&b, d, s)
		}
	}
	_workload_end(&cfg, &w3)

	// Workload 4: 1,024-Actor Swarm Scaling
	w4 := Workload_Section{
		group = "workload_4_swarm",
		title = fmt.tprintf("WORKLOAD 4: 1,024-Actor Swarm Scaling (%d messages across %d actors)", TOTAL_MESSAGES, NUM_SWARM_ACTORS),
	}
	for b := bench_begin(&bctx, &w4, "3g_work_stealing_swarm_1024", "3g. Sharded Work-Stealing M:N Swarm (1,024)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_work_stealing_swarm(TOTAL_MESSAGES, NUM_PRODUCERS, NUM_SWARM_ACTORS)
		bench_sample(&b, d, s)
	}
	when USE_LIBPONYRT {
		if !cfg.quiet && _matches_bench_filter(&cfg, "workload_4_swarm", "workload_4_swarm/4d_libponyrt_swarm", "4d. Official Pony (`libponyrt` 0.77.2 M:N Swarm)") {
			spawn_self_mode("--child-pony-w4")
		}
	}
	for b := bench_begin(&bctx, &w4, "5b_actor_swarm_chain_return", "5b. `package actor` M:N Swarm (Chain Return)", TOTAL_MESSAGES); bench_next(&b); {
		d, s := bench_library_swarm(TOTAL_MESSAGES, NUM_PRODUCERS, NUM_SWARM_ACTORS)
		bench_sample(&b, d, s)
	}
	_workload_end(&cfg, &w4)

	// Workload 5: Autonomous Actor-to-Actor Graph + CPU Work
	w5 := Workload_Section{
		group = "workload_5_a2a",
		title = fmt.tprintf("WORKLOAD 5: Autonomous Actor-to-Actor Graph + CPU Work (%d hops, %d actors)", A2A_TOTAL_HOPS, NUM_SWARM_ACTORS),
	}
	worker_counts := [?]int{1, 2, 4, 8, 16}
	for w in worker_counts {
		bname := fmt.tprintf("3h_pony_mn_a2a_%dw", w)
		label := fmt.tprintf("3h. Pony M:N Actor-to-Actor + CPU (%d %s)", w, "Worker" if w == 1 else "Workers")
		for b := bench_begin(&bctx, &w5, bname, label, A2A_TOTAL_HOPS, threads = w, has_threads = true); bench_next(&b); {
			d, s := bench_pony_mn_a2a(A2A_TOTAL_HOPS, w)
			bench_sample(&b, d, s)
		}
	}
	when USE_LIBPONYRT {
		if !cfg.quiet {
			pony_w5_modes := [?]struct{ flag: cstring, w: int }{
				{"--child-pony-w5-1", 1},
				{"--child-pony-w5-2", 2},
				{"--child-pony-w5-4", 4},
				{"--child-pony-w5-8", 8},
			}
			for entry in pony_w5_modes {
				fn := fmt.tprintf("workload_5_a2a/4e_libponyrt_a2a_%dw", entry.w)
				lbl := fmt.tprintf("4e. Official Pony (`libponyrt` 0.77.2, %d %s)", entry.w, "Thread" if entry.w == 1 else "Threads")
				if _matches_bench_filter(&cfg, "workload_5_a2a", fn, lbl) {
					spawn_self_mode(entry.flag)
				}
			}
		}
	}
	for w in worker_counts {
		bname := fmt.tprintf("5c_actor_a2a_%dw", w)
		label := fmt.tprintf("5c. `package actor` A2A + CPU (%d %s)", w, "Worker" if w == 1 else "Workers")
		for b := bench_begin(&bctx, &w5, bname, label, A2A_TOTAL_HOPS, threads = w, has_threads = true); bench_next(&b); {
			d, s := bench_library_a2a(A2A_TOTAL_HOPS, w)
			bench_sample(&b, d, s)
		}
	}
	_workload_end(&cfg, &w5)
	free_all(context.temp_allocator)

	if cfg.packet_out_path != "" {
		ts_unix := i64(time.to_unix_nanoseconds(time.now())) / 1_000_000_000
		pkt := hugi.Run_Packet{
			suite_name     = strings.clone("sleipnir_actor_benchmark"),
			lang           = strings.clone(cfg.lang),
			commit         = strings.clone(cfg.commit),
			run_id         = strings.clone(cfg.run_id),
			os             = strings.clone(ODIN_OS_STRING),
			arch           = strings.clone(ODIN_ARCH_STRING),
			optimization   = strings.clone(fmt.tprintf("%v", ODIN_OPTIMIZATION_MODE)),
			odin_version   = strings.clone(ODIN_VERSION),
			timestamp_unix = ts_unix,
			selective      = (cfg.groups_filter != "" && cfg.groups_filter != "all") || cfg.name_filter != "",
			records        = bctx.records,
		}
		bctx.records = nil
		defer hugi.packet_destroy(&pkt)

		if err := hugi.write_packet_file(cfg.packet_out_path, pkt); err != nil {
			fmt.eprintfln("actor_benchmark: failed to write hugi packet '%s': %v", cfg.packet_out_path, err)
			os.exit(1)
		}
	}
}
