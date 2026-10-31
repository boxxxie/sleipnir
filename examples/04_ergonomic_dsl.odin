// =============================================================================
// Example 04: Ergonomic Actor DSL (`scoped_scheduler`, `call`/`reply`, Auto-Pools)
//
// Demonstrates Sleipnir's zero-ceremony non-AST API:
//   1. `actor.scoped_scheduler(num_workers = 2)` (`@(deferred_out)` RAII lifecycle)
//   2. Automatic per-message-type `Envelope_Pool` management (no manual pool init/destroy)
//   3. `actor.call` + `actor.reply` — request/reply without putting `reply_to` in `msg`!
//   4. Multi-behavior actors via tagged `union` value handlers (`proc(self: ^State, msg: Msg)`)
//   5. Compile-time structural typing (`actor.spawn_self` via `base:intrinsics`)
//
// Run:
//   odin run examples/04_ergonomic_dsl.odin -file -o:speed
// =============================================================================

package main

import "../actor"
import "core:fmt"

// --- 1. Multi-Behavior Actor via Tagged Union (Zero `reply_to` in Message!) ---

Deposit     :: struct { amount: i64 }
Withdraw    :: struct { amount: i64 }
Get_Balance :: struct {}

Account_Msg :: union {
	Deposit,
	Withdraw,
	Get_Balance,
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
	case Get_Balance:
		// Reply directly via envelope header metadata — works with both
		// synchronous `actor.call` and async `actor.send(target, msg, reply_to)`!
		actor.reply(self.balance)
	}
}

// --- 2. Self-Contained Actor Struct (`base:intrinsics` Structural Typing) ---

Balance_Printer :: struct {
	last_printed: i64,
	receive:      proc(self: ^Balance_Printer, balance: i64),
}

main :: proc() {
	// 1-line RAII scheduler: starts 2 worker threads and auto-cleans up at scope exit
	_ = actor.scoped_scheduler(num_workers = 2)

	// Spawn a multi-behavior `Bank_Account` actor — `Envelope_Pool(Account_Msg)` is created automatically!
	account := actor.spawn(Bank_Account{owner = "Odin", balance = 100}, account_behavior)

	// Send a batch of 3 messages in one atomic XCHG without passing `&sched` or `&pool`
	batch := [3]Account_Msg{
		Deposit{amount = 50},
		Deposit{amount = 25},
		Withdraw{amount = 30},
	}
	actor.send(account, batch[:])

	// 1) Synchronous request/reply (`actor.call`) — returns `i64` directly!
	bal := actor.call(account, Account_Msg(Get_Balance{}), i64)
	fmt.printfln("Queried balance via `actor.call`: $%d", bal)

	// 2) Async `reply_to` in envelope header (`actor.send(account, msg, printer)`)
	printer := actor.spawn_self(Balance_Printer{
		receive = proc(self: ^Balance_Printer, balance: i64) {
			self.last_printed = balance
			fmt.printfln("Printer actor received async reply: $%d", balance)
		},
	})
	actor.send(account, Account_Msg(Get_Balance{}), printer)
	actor.wait_messages(5)
	actor.wait_idle()
}
