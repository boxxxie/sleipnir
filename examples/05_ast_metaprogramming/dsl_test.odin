// =============================================================================
// End-to-End Runtime Verification Tests for `@(actor)` AST Metaprogramming DSL
//
// Run via:
//   odin test examples/05_ast_metaprogramming -custom-attribute:actor,behavior
// =============================================================================

package main

import "../../actor"
import "core:mem"
import "core:testing"

@(test)
test_ast_dsl_selector_calls_default_args_and_overloads :: proc(t: ^testing.T) {
	track: mem.Tracking_Allocator
	mem.tracking_allocator_init(&track, context.allocator)
	defer mem.tracking_allocator_destroy(&track)
	saved := context.allocator
	context.allocator = mem.tracking_allocator(&track)
	defer {
		context.allocator = saved
		testing.expect_value(t, len(track.allocation_map), 0)
		testing.expect_value(t, len(track.bad_free_array), 0)
	}

	{
		_ = actor.scoped_scheduler(num_workers = 2)

		logger  := spawn_audit_logger(Audit_Logger{})
		account := spawn_bank_account(Bank_Account{owner = "Sleipnir", balance = 500})

		// 1. Selector `->` 1-way sends (including default parameter `memo = "deposit"`):
		account->deposit(250)
		account->deposit(100, "wire-in")
		account->withdraw(175)

		// 2. Selector `->` synchronous request/reply (`get_balance() -> i64`):
		bal1 := account->get_balance()
		testing.expect_value(t, bal1, 675)

		// 3. `send_<behavior>` and `call_<behavior>` overload groups (both Actor and PID):
		send_deposit(account, 25)
		send_deposit(actor.pid(account.raw), 50, "bonus")
		testing.expect_value(t, call_get_balance(account), 750)
		testing.expect_value(t, call_get_balance(actor.pid(account.raw)), 750)

		// 4. Async `send_<behavior>(target, ..., reply_to)` forwarding a return value to another actor:
		reply_sink := actor.spawn(i64(0), proc(state: ^i64, v: i64) {
			state^ = v
		})
		send_get_balance(account, actor.pid(reply_sink))

		// 5. Cross-actor message passing (`audit_snapshot` -> `logger->record_snapshot(...)`):
		account->audit_snapshot(logger)
		actor.wait_idle()

		testing.expect_value(t, reply_sink.state, 750)
		testing.expect_value(t, logger->get_entries_logged(), 1)
		testing.expect_value(t, logger.state.last_balance, 750)

		// 6. Shared `@(behavior="reset")` overload group across both `Audit_Logger` and `Bank_Account`:
		logger->reset()
		send_reset(account)
		testing.expect_value(t, account->get_balance(), 0)
		testing.expect_value(t, logger->get_entries_logged(), 0)
	}
}
