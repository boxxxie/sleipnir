// =============================================================================
// Example 01: Minimal Two-Actor Ping-Pong
//
// Demonstrates spawning two typed `actor.Actor` instances on a shared M:N
// `actor.Scheduler` and exchanging messages using `actor.forward` (`iso`
// zero-copy token handoff).
//
// Run:
//   odin run examples/01_ping_pong.odin -file -o:speed
// =============================================================================

package main

import "../actor"
import "core:fmt"
import "core:time"

Ball_Msg :: struct {
	bounces:        i64,
	target_bounces: i64,
}

Table_State :: struct {
	name:    string,
	partner: ^actor.Actor(Table_State, Ball_Msg),
}

player_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Table_State, Ball_Msg), msg: ^Ball_Msg) {
	if msg.bounces < msg.target_bounces {
		// Zero-copy `iso` forward of the Ball_Msg envelope to our partner actor
		actor.forward(ctx, self.state.partner, Ball_Msg{
			bounces        = msg.bounces + 1,
			target_bounces = msg.target_bounces,
		})
	} else {
		actor.complete_token(ctx, 1)
	}
}

main :: proc() {
	sched: actor.Scheduler
	actor.init(&sched, actor.Scheduler_Config{
		num_workers    = 2,
		enable_parking = true,
	})
	defer actor.destroy(&sched)

	ping := actor.spawn(Table_State{name = "Ping"}, player_receive)
	pong := actor.spawn(Table_State{name = "Pong"}, player_receive)
	defer actor.destroy(ping)
	defer actor.destroy(pong)

	ping.state.partner = pong
	pong.state.partner = ping

	total_bounces :: 200_000
	ball_env := actor.Envelope(Ball_Msg){
		flags = actor.ENVELOPE_FLAG_STATIC,
		msg   = Ball_Msg{bounces = 0, target_bounces = total_bounces},
	}

	sched.pending_tokens = 1
	start := time.tick_now()
	actor.scheduler_start(&sched)
	actor.send(&sched, ping, &ball_env)
	actor.scheduler_wait_tokens(&sched)
	elapsed := time.tick_since(start)

	sec := time.duration_seconds(elapsed)
	fmt.printfln(
		"Completed %d actor-to-actor bounces in %.2f ms (%.1f ns/hop)",
		ball_env.msg.bounces,
		sec * 1000.0,
		(sec * 1e9) / f64(total_bounces),
	)
}
