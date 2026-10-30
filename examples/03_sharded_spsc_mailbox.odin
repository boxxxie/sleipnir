// =============================================================================
// Example 03: Wait-Free Sharded SPSC Mailbox (`Sharded_SPSC_Mailbox`)
//
// Demonstrates the ultra-low-latency Sharded SPSC Mailbox architecture
// (~900+ M msg/s, zero `lock xchg` RMW instructions) for fixed-producer fan-in.
//
// Run:
//   odin run examples/03_sharded_spsc_mailbox.odin -file -o:speed
// =============================================================================

package main

import "../actor"
import "base:intrinsics"
import "core:fmt"
import "core:sync"
import "core:thread"
import "core:time"

NUM_PRODUCERS  :: 4
TOTAL_MESSAGES :: 2_000_000

Producer_Ctx :: struct {
	mbox:    ^actor.Sharded_SPSC_Mailbox(i64, 8192),
	prod_id: int,
	count:   int,
	wg:      ^sync.Wait_Group,
}

producer_proc :: proc(t: ^thread.Thread) {
	ctx := cast(^Producer_Ctx)t.data
	for i := 0; i < ctx.count; i += 1 {
		actor.push(ctx.mbox, ctx.prod_id, 1)
	}
	sync.wait_group_done(ctx.wg)
}

main :: proc() {
	mbox: actor.Sharded_SPSC_Mailbox(i64, 8192)
	actor.init(&mbox, NUM_PRODUCERS)
	defer actor.destroy(&mbox)

	wg: sync.Wait_Group
	sync.wait_group_add(&wg, NUM_PRODUCERS)

	producers := make([]^thread.Thread, NUM_PRODUCERS)
	ctxs      := make([]Producer_Ctx, NUM_PRODUCERS)
	defer delete(producers)
	defer delete(ctxs)

	msgs_per_prod := TOTAL_MESSAGES / NUM_PRODUCERS
	start := time.tick_now()

	for p := 0; p < NUM_PRODUCERS; p += 1 {
		ctxs[p] = Producer_Ctx{
			mbox    = &mbox,
			prod_id = p,
			count   = msgs_per_prod,
			wg      = &wg,
		}
		producers[p] = thread.create(producer_proc)
		producers[p].data = &ctxs[p]
		thread.start(producers[p])
	}

	received := 0
	total_score: i64 = 0
	for received < TOTAL_MESSAGES {
		n := actor.sharded_spsc_consume(&mbox, 128, &total_score, proc(acc: ^i64, msg: ^i64) {
			acc^ += msg^
		})
		if n == 0 {
			intrinsics.cpu_relax()
		} else {
			received += n
		}
	}

	sync.wait_group_wait(&wg)
	elapsed := time.tick_since(start)

	for p := 0; p < NUM_PRODUCERS; p += 1 {
		thread.destroy(producers[p])
	}

	sec := time.duration_seconds(elapsed)
	fmt.printfln(
		"Sharded_SPSC_Mailbox processed %d msgs in %.2f ms (%.2f M msg/s | %.2f ns/msg) | Checksum = %d",
		received,
		sec * 1000.0,
		(f64(TOTAL_MESSAGES) / sec) / 1e6,
		(sec * 1e9) / f64(TOTAL_MESSAGES),
		total_score,
	)
}
