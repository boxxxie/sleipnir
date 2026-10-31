// =============================================================================
// Example 05: Multi-File `@(actor)` AST Metaprogramming (`->`, `send_`, `call_`)
//
// Demonstrates:
//   1. Only `@(actor)` is needed on structs — procedures taking `self: ^Actor`
//      are automatically discovered across all `.odin` files in the package!
//   2. Procedures returning a value (`get_balance :: proc(...) -> i64`) need
//      zero `reply_to` parameters and work via `account->get_balance()` or
//      `call_get_balance(account)`!
//   3. Built-in Odin `->` method syntax (`account->deposit(250)`) alongside
//      `send_deposit(account, 250)` and unified `send_reset` overload groups.
//
// Generate & Run:
//   odin run tools/sleipnir_gen -- examples/05_ast_metaprogramming
//   odin run examples/05_ast_metaprogramming -custom-attribute:actor,behavior -o:speed
// =============================================================================

package main

import "../../actor"
import "core:fmt"

main :: proc() {
	_ = actor.scoped_scheduler(num_workers = 2)

	logger  := spawn_audit_logger(Audit_Logger{})
	account := spawn_bank_account(Bank_Account{owner = "Sleipnir", balance = 500})

	// 1. Built-in Odin `->` selector call syntax (supports default parameters!):
	account->deposit(250)               // Uses default `memo = "deposit"`
	account->deposit(100, "wire-in")    // Explicit `memo`
	account->withdraw(175)

	// 2. Request/Reply return value (`get_balance :: proc(self: ^Bank_Account) -> i64`):
	balance := account->get_balance()
	fmt.printfln("Queried balance via `account->get_balance()`: $%d", balance)

	// 3. `send_` and `call_` function syntax (including cross-actor overload groups):
	send_deposit(account, 25)
	send_audit_snapshot(account, logger)
	fmt.printfln("Updated balance via `call_get_balance(account)`: $%d (audit entries: %d)",
		call_get_balance(account), logger->get_entries_logged())

	// Both `Audit_Logger` and `Bank_Account` share `send_reset`:
	send_reset(logger)
	send_reset(account)
}
