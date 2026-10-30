// =============================================================================
// Example 02: Heterogeneous Actor Pipeline (`Worker` -> `Aggregator`)
//
// Demonstrates running multiple distinct `actor.Actor($State, $Msg)` types on
// the same M:N `actor.Scheduler` with `Envelope_Pool` batched memory recycling
// and adaptive `sync.Futex` worker parking (`enable_parking = true`).
//
// Run:
//   odin run examples/02_heterogeneous_pipeline.odin -file -o:speed
// =============================================================================

package main

import "../actor"
import "core:fmt"

Work_Order :: union {
	Compute_Chunk,
	Flush_Subtotal,
}

Compute_Chunk :: struct {
	value: i64,
}

Flush_Subtotal :: struct {
	reply_to: ^actor.Actor(Aggregator_State, Subtotal_Report),
}

Subtotal_Report :: struct {
	worker_actor_id: u32,
	subtotal:        i64,
}

Worker_State :: struct {
	accum: i64,
}

Aggregator_State :: struct {
	grand_total:  i64,
	reports_seen: int,
}

worker_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Worker_State, Work_Order), msg: ^Work_Order) {
	switch m in msg {
	case Compute_Chunk:
		self.state.accum += m.value * m.value
	case Flush_Subtotal:
		actor.send(ctx, m.reply_to, Subtotal_Report{
			worker_actor_id = self.id,
			subtotal        = self.state.accum,
		})
	}
}

aggregator_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Aggregator_State, Subtotal_Report), msg: ^Subtotal_Report) {
	self.state.grand_total += msg.subtotal
	self.state.reports_seen += 1
	fmt.printfln("  -> Aggregator received subtotal %d from Worker Actor #%d", msg.subtotal, msg.worker_actor_id)
	actor.complete_token(ctx, 1)
}

main :: proc() {
	num_os_workers :: 4
	num_actor_workers :: 8

	sched: actor.Scheduler
	actor.init(&sched, actor.Scheduler_Config{
		num_workers    = num_os_workers,
		enable_parking = true,
	})
	defer actor.destroy(&sched)

	order_pools  := make([]actor.Envelope_Pool(Work_Order), num_os_workers)
	report_pools := make([]actor.Envelope_Pool(Subtotal_Report), num_os_workers)
	for w := 0; w < num_os_workers; w += 1 {
		actor.init(&order_pools[w], u16(w), 256)
		actor.init(&report_pools[w], u16(w), 256)
	}
	defer {
		for w := 0; w < num_os_workers; w += 1 {
			actor.destroy(&order_pools[w])
			actor.destroy(&report_pools[w])
		}
		delete(order_pools)
		delete(report_pools)
	}

	aggregator := actor.spawn(Aggregator_State{}, aggregator_receive, report_pools, id = 100)
	defer actor.destroy(aggregator)

	workers := make([]actor.Actor(Worker_State, Work_Order), num_actor_workers)
	defer delete(workers)
	for i := 0; i < num_actor_workers; i += 1 {
		actor.init(&workers[i], Worker_State{}, worker_receive, order_pools, id = u32(i))
	}

	sched.pending_tokens = i64(num_actor_workers)
	actor.scheduler_start(&sched)

	for i := 0; i < num_actor_workers; i += 1 {
		for v: i64 = 1; v <= 10; v += 1 {
			actor.send(&sched, &order_pools[0], &workers[i], Work_Order(Compute_Chunk{value = v}))
		}
		actor.send(&sched, &order_pools[0], &workers[i], Work_Order(Flush_Subtotal{reply_to = aggregator}))
	}

	actor.scheduler_wait_tokens(&sched)
	actor.scheduler_wait_idle(&sched)

	// Sum of squares 1^2..10^2 = 385; 385 * 8 actors = 3080
	fmt.printfln("Grand Total across %d actors = %d (expected 3080)",
		aggregator.state.reports_seen, aggregator.state.grand_total)
}
