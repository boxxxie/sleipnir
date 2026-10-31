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

// =============================================================================
// Layer 1: Executable Specification
// =============================================================================

@(test)
test_ast_dsl_selector_calls_default_args_and_overloads :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		s := banking_scenario(t, actor.scoped_scheduler(num_workers = 2))

		given_audit_logger(&s)
		given_bank_account(&s, owner = "Sleipnir", balance = 500)

		// 1. Selector `->` 1-way sends (default `memo` and explicit `memo`) & withdrawal:
		upon_depositing_via_selector(&s, 250)
		upon_depositing_via_selector(&s, 100, memo = "wire-in")
		upon_withdrawing_via_selector(&s, 175)

		// 2. Synchronous & asynchronous balance verification:
		then_balance_is(&s, 675)

		// 3. `send_deposit` overload group across both Actor handle and raw PID:
		upon_sending_deposit(&s, 25, via = .Actor_Handle)
		upon_sending_deposit(&s, 50, memo = "bonus", via = .PID)

		// 4. Verify updated balance across `->`, `call_get_balance` (Actor + PID),
		//    and async `send_get_balance` (`_reply_actor` + `_reply_pid`):
		then_balance_is(&s, 750)

		// 5. Cross-actor message passing (`audit_snapshot` -> `logger->record_snapshot(...)`):
		upon_recording_audit_snapshot(&s)
		then_audit_log_matches(&s, entries = 1, last_balance = 750)

		// 6. Shared `@(behavior="reset")` overload group across both `Audit_Logger` and `Bank_Account`:
		upon_resetting_logger_and_account(&s)
		then_balance_is(&s, 0)
		then_audit_log_matches(&s, entries = 0, last_balance = 0)
	})
}

// =============================================================================
// Layer 2 & 3: Banking DSL & Actor Protocol Driver
// =============================================================================

@(private = "file")
Addressing_Mode :: enum {
	Actor_Handle,
	PID,
}

@(private = "file")
Banking_Scenario :: struct {
	t:       ^testing.T,
	sched:   ^actor.Scheduler,
	logger:  Audit_Logger_Actor,
	account: Bank_Account_Actor,
}

@(private = "file")
with_leak_check :: proc(t: ^testing.T, body: proc(t: ^testing.T)) {
	track: mem.Tracking_Allocator
	mem.tracking_allocator_init(&track, context.allocator)
	defer mem.tracking_allocator_destroy(&track)
	defer free_all(context.temp_allocator)

	saved_allocator := context.allocator
	context.allocator = mem.tracking_allocator(&track)
	body(t)
	context.allocator = saved_allocator

	testing.expect_value(t, len(track.allocation_map), 0)
	testing.expect_value(t, len(track.bad_free_array), 0)
}

@(private = "file")
banking_scenario :: proc(t: ^testing.T, sched: ^actor.Scheduler) -> Banking_Scenario {
	return Banking_Scenario{
		t     = t,
		sched = sched,
	}
}

@(private = "file")
given_audit_logger :: proc(s: ^Banking_Scenario) {
	s.logger = spawn_audit_logger(Audit_Logger{})
}

@(private = "file")
given_bank_account :: proc(s: ^Banking_Scenario, owner: string, balance: i64) {
	s.account = spawn_bank_account(Bank_Account{owner = owner, balance = balance})
}

@(private = "file")
upon_depositing_via_selector :: proc(s: ^Banking_Scenario, amount: i64, memo: string = "") {
	if memo == "" {
		// Exercises generated default parameter `memo = "deposit"`
		s.account->deposit(amount)
	} else {
		s.account->deposit(amount, memo)
	}
}

@(private = "file")
upon_withdrawing_via_selector :: proc(s: ^Banking_Scenario, amount: i64) {
	s.account->withdraw(amount)
}

@(private = "file")
upon_sending_deposit :: proc(
	s:      ^Banking_Scenario,
	amount: i64,
	via:    Addressing_Mode,
	memo:   string = "",
) {
	switch via {
	case .Actor_Handle:
		if memo == "" {
			send_deposit(s.account, amount)
		} else {
			send_deposit(s.account, amount, memo)
		}
	case .PID:
		if memo == "" {
			send_deposit(actor.pid(s.account.raw), amount)
		} else {
			send_deposit(actor.pid(s.account.raw), amount, memo)
		}
	}
}

@(private = "file")
upon_recording_audit_snapshot :: proc(s: ^Banking_Scenario) {
	s.account->audit_snapshot(s.logger)
	actor.wait_idle()
}

@(private = "file")
upon_resetting_logger_and_account :: proc(s: ^Banking_Scenario) {
	s.logger->reset()
	send_reset(s.account)
}

// Cross-cutting driver check: verifies selector `->`, `call_get_balance` (Actor + PID),
// and async `send_get_balance` (`_reply_actor` + `_reply_pid`) in a single assertion.
@(private = "file")
then_balance_is :: proc(s: ^Banking_Scenario, expected: i64) {
	// 1. Synchronous selector & `call_get_balance` overloads:
	testing.expect_value(s.t, s.account->get_balance(), expected)
	testing.expect_value(s.t, call_get_balance(s.account), expected)
	testing.expect_value(s.t, call_get_balance(actor.pid(s.account.raw)), expected)

	// 2. Async `send_get_balance` (`_reply_actor` and `_reply_pid`) overloads:
	reply_actor_sink := actor.spawn(i64(-1), proc(state: ^i64, v: i64) {
		state^ = v
	})
	reply_pid_sink := actor.spawn(i64(-1), proc(state: ^i64, v: i64) {
		state^ = v
	})

	send_get_balance(s.account, actor.pid(reply_actor_sink))
	send_get_balance(actor.pid(s.account.raw), actor.pid(reply_pid_sink))
	actor.wait_idle()

	testing.expect_value(s.t, reply_actor_sink.state, expected)
	testing.expect_value(s.t, reply_pid_sink.state, expected)
}

// Verifies `Audit_Logger` state, selector `->`, `call_get_entries_logged` (Actor + PID),
// and async `send_get_entries_logged` (`_reply_actor` + `_reply_pid`) overloads:
@(private = "file")
then_audit_log_matches :: proc(s: ^Banking_Scenario, entries: int, last_balance: i64) {
	// 1. Synchronous selector, `call_get_entries_logged` overloads, & state check:
	testing.expect_value(s.t, s.logger->get_entries_logged(), entries)
	testing.expect_value(s.t, call_get_entries_logged(s.logger), entries)
	testing.expect_value(s.t, call_get_entries_logged(actor.pid(s.logger.raw)), entries)
	testing.expect_value(s.t, s.logger.state.last_balance, last_balance)

	// 2. Async `send_get_entries_logged` (`_reply_actor` and `_reply_pid`) overloads:
	entries_actor_sink := actor.spawn(int(-1), proc(state: ^int, v: int) {
		state^ = v
	})
	entries_pid_sink := actor.spawn(int(-1), proc(state: ^int, v: int) {
		state^ = v
	})

	send_get_entries_logged(s.logger, actor.pid(entries_actor_sink))
	send_get_entries_logged(actor.pid(s.logger.raw), actor.pid(entries_pid_sink))
	actor.wait_idle()

	testing.expect_value(s.t, entries_actor_sink.state, entries)
	testing.expect_value(s.t, entries_pid_sink.state, entries)
}
