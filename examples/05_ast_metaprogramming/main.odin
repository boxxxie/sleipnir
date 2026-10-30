// =============================================================================
// Example 05: Pony-Style `@(actor)` & `@(behavior)` AST Metaprogramming
//
// Demonstrates defining actors and behaviors using Odin custom attributes
// `@(actor)` and `@(behavior)`. Running `tools/sleipnir_gen` parses this file
// with `core:odin/parser` and generates `actors_gen.odin` containing:
//   - Message structs (`Bank_Account_Msg_deposit`, etc.)
//   - Tagged union (`Bank_Account_Msg`) & `PID` alias (`Bank_Account_PID`)
//   - Dispatcher (`bank_account_dispatch`)
//   - Constructor (`spawn_bank_account`)
//   - Async behavior callers (`be_deposit`, `be_withdraw`, `be_audit_snapshot`)
//
// Generate & Run:
//   odin run tools/sleipnir_gen -- examples/05_ast_metaprogramming
//   odin run examples/05_ast_metaprogramming -custom-attribute:actor,behavior -o:speed
// =============================================================================

package main

import "../../actor"
import "core:fmt"

// --- Actor 1: Audit Logger ---

@(actor)
Audit_Logger :: struct {
	entries_logged: int,
	last_balance:   i64,
}

@(behavior)
record_snapshot :: proc(self: ^Audit_Logger, owner: string, balance: i64, tx_count: int) {
	self.entries_logged += 1
	self.last_balance    = balance
	fmt.printfln("[Audit #%d] Account '%s' balance = $%d (after %d transactions)",
		self.entries_logged, owner, balance, tx_count)
	actor.complete_token()
}

// --- Actor 2: Bank Account ---

@(actor)
Bank_Account :: struct {
	owner:    string,
	balance:  i64,
	tx_count: int,
}

@(behavior)
deposit :: proc(self: ^Bank_Account, amount: i64, memo: string = "deposit") {
	self.balance  += amount
	self.tx_count += 1
}

@(behavior)
withdraw :: proc(self: ^Bank_Account, amount: i64) {
	self.balance  -= amount
	self.tx_count += 1
}

@(behavior)
reset_account :: proc(self: ^Bank_Account) {
	self.balance  = 0
	self.tx_count = 0
}

@(behavior)
audit_snapshot :: proc(self: ^Bank_Account, logger: Audit_Logger_PID) {
	// Call a Pony-style behavior on another generated actor (`logger`)!
	be_record_snapshot(logger, self.owner, self.balance, self.tx_count)
}

main :: proc() {
	sched := actor.scoped_scheduler(num_workers = 2)
	sched.pending_tokens = 1

	logger  := spawn_audit_logger(Audit_Logger{})
	account := spawn_bank_account(Bank_Account{owner = "Sleipnir", balance = 500})

	// Call generated Pony-style `be_*` behaviors directly!
	be_deposit(account, 250)               // Uses default `memo = "deposit"`
	be_deposit(account, 100, "wire-in")    // Explicit `memo`
	be_withdraw(account, 175)
	be_audit_snapshot(account, actor.pid(logger))

	actor.wait_tokens()
	actor.wait_idle()
}
