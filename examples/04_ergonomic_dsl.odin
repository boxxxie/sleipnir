// =============================================================================
// Example 04: Ergonomic Actor DSL (`scoped_scheduler`, Ambient Context, Auto-Pools, `PID`)
//
// Demonstrates Sleipnir's zero-ceremony ergonomic API:
//   1. `actor.scoped_scheduler(num_workers = 2)` (`@(deferred_out)` RAII lifecycle)
//   2. Automatic per-message-type `Envelope_Pool` management (no manual pool init/destroy)
//   3. Ambient `actor.send(target, msg)` and `actor.forward(target, msg)` (no `&sched` or `ctx` threading)
//   4. Decoupled `actor.PID(Msg)` handles (`actor.pid(a)`)
//   5. Multi-behavior actors via tagged `union` value handlers (`proc(self: ^State, msg: Msg)`)
//   6. Compile-time structural typing (`actor.spawn_self` via `base:intrinsics`)
//
// Run:
//   odin run examples/04_ergonomic_dsl.odin -file -o:speed
// =============================================================================

package main

import "../actor"
import "core:fmt"

// --- 1. Pony-Style Multi-Behavior Actor via Tagged Union ---

Deposit  :: struct { amount: i64 }
Withdraw :: struct { amount: i64 }
Query    :: struct { reply_to: actor.PID(i64) }

Account_Msg :: union {
	Deposit,
	Withdraw,
	Query,
}

Bank_Account :: struct {
	owner:   string,
	balance: i64,
}

account_behavior :: proc(self: ^Bank_Account, msg: Account_Msg) {
	switch m in msg {
	case Deposit:
		self.balance += m.amount
	case Withdraw:
		self.balance -= m.amount
	case Query:
		// Ambient `actor.send` to a `PID(i64)` handle — no `ctx` or `&pool` needed!
		actor.send(m.reply_to, self.balance)
	}
}

// --- 2. Self-Contained Actor Struct (`base:intrinsics` Structural Typing) ---

Balance_Printer :: struct {
	printed_balance: i64,
	receive:         proc(self: ^Balance_Printer, balance: i64),
}

main :: proc() {
	// 1-line RAII scheduler: starts 2 worker threads and auto-cleans up at scope exit
	sched := actor.scoped_scheduler(num_workers = 2)
	sched.pending_tokens = 1

	// Spawn a self-contained struct with embedded `receive` field via `actor.spawn_self`
	printer := actor.spawn_self(Balance_Printer{
		receive = proc(self: ^Balance_Printer, balance: i64) {
			self.printed_balance = balance
			fmt.printfln("Account final balance: $%d", balance)
			actor.complete_token() // Ambient 0-arg `complete_token()`
		},
	})

	// Spawn a multi-behavior `Bank_Account` actor — `Envelope_Pool(Account_Msg)` is created automatically!
	account := actor.spawn(Bank_Account{owner = "Odin", balance = 100}, account_behavior)

	// Send single messages and a batch slice without passing `&sched` or `&pool`
	batch := [3]Account_Msg{
		Deposit{amount = 50},
		Deposit{amount = 25},
		Withdraw{amount = 30},
	}
	actor.send(account, batch[:])
	actor.send(account, Account_Msg(Query{reply_to = actor.pid(printer)}))

	actor.wait_tokens()
	actor.wait_idle()
}
