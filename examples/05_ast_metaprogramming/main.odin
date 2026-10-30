// =============================================================================
// Example 05: Multi-File Pony-Style `@(actor)` & `@(behavior)` AST Metaprogramming
//
// Demonstrates defining `@(actor)` structs and `@(behavior)` procedures across
// multiple `.odin` files (`audit_logger.odin`, `bank_account.odin`, `main.odin`).
// Running `tools/sleipnir_gen` scans the entire package directory and emits
// `actors_gen.odin` with unified `be_*` overload groups.
//
// Generate & Run:
//   odin run tools/sleipnir_gen -- examples/05_ast_metaprogramming
//   odin run examples/05_ast_metaprogramming -custom-attribute:actor,behavior -o:speed
// =============================================================================

package main

import "../../actor"

main :: proc() {
	sched := actor.scoped_scheduler(num_workers = 2)
	sched.pending_tokens = 1

	logger  := spawn_audit_logger(Audit_Logger{})
	account := spawn_bank_account(Bank_Account{owner = "Sleipnir", balance = 500})

	// Both `Audit_Logger` and `Bank_Account` support `be_reset` via unified proc overloading!
	be_reset(logger)

	// Call generated Pony-style `be_*` behaviors directly!
	be_deposit(account, 250)               // Uses default `memo = "deposit"`
	be_deposit(account, 100, "wire-in")    // Explicit `memo`
	be_withdraw(account, 175)
	be_audit_snapshot(account, actor.pid(logger))

	actor.wait_tokens()
	actor.wait_idle()
}
