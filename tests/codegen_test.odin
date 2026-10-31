package sleipnir_tests

import "core:strings"
import "core:testing"
import "../actor/codegen"

@(test)
test_01_ast_codegen_parses_actor_and_behaviors :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		// Notice: `@(behavior)` is completely optional once a struct is marked `@(actor)`!
		sample_src :: `
package demo

import "../actor"

@(actor)
Bank_Account :: struct {
	owner:   string,
	balance: i64,
}

deposit :: proc(self: ^Bank_Account, amount: i64, memo: string = "cash") {
	self.balance += amount
}

reset_balance :: proc(self: ^Bank_Account) {
	self.balance = 0
}

get_balance :: proc(self: ^Bank_Account) -> i64 {
	return self.balance
}
`

		generated, ok := codegen.generate_from_source(sample_src, "bank.odin")
		defer delete(generated)

		testing.expect(t, ok)
		testing.expect(t, strings.contains(generated, "package demo"))
		testing.expect(t, strings.contains(generated, "Bank_Account_Msg_deposit :: struct"))
		testing.expect(t, strings.contains(generated, "Bank_Account_Msg_reset_balance :: struct {}"))
		testing.expect(t, strings.contains(generated, "Bank_Account_Msg_get_balance :: struct {}"))
		testing.expect(t, strings.contains(generated, "Bank_Account_Msg :: union"))
		testing.expect(t, strings.contains(generated, "Bank_Account_VTable :: struct"))
		testing.expect(t, strings.contains(generated, "Bank_Account_Actor :: struct"))
		testing.expect(t, strings.contains(generated, "Bank_Account_PID :: Bank_Account_Actor"))
		testing.expect(t, strings.contains(generated, "bank_account_dispatch :: proc(ctx: ^actor.Context, self: ^Bank_Account, msg: ^Bank_Account_Msg)"))
		testing.expect(t, strings.contains(generated, "deposit(self, m.amount, m.memo)"))
		testing.expect(t, strings.contains(generated, "reset_balance(self)"))
		testing.expect(t, strings.contains(generated, "res := get_balance(self)"))
		testing.expect(t, strings.contains(generated, "actor.reply(ctx, res)"))
		testing.expect(t, strings.contains(generated, "spawn_bank_account :: proc"))
		testing.expect(t, strings.contains(generated, "send_bank_account_deposit_actor :: #force_inline proc(target: Bank_Account_Actor, amount: i64, memo: string = \"cash\")"))
		testing.expect(t, strings.contains(generated, "call_bank_account_get_balance_actor :: #force_inline proc(target: Bank_Account_Actor) -> i64"))
		testing.expect(t, strings.contains(generated, "send_deposit :: proc {"))
		testing.expect(t, strings.contains(generated, "send_reset_balance :: proc {"))
		testing.expect(t, strings.contains(generated, "call_get_balance :: proc {"))
		testing.expect(t, strings.contains(generated, "send_get_balance :: proc {"))
	})
}

@(test)
test_02_multi_file_ast_codegen_cross_file_behaviors_and_overloads :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		// File 1: Defines `Bank_Account` struct, but its behaviors live in File 2!
		file_1 :: `
package banking

import "../actor"

@(actor)
Bank_Account :: struct {
	balance: i64,
}
`

		// File 2: Defines behaviors for `Bank_Account` AND a second actor `Audit_Timer`
		// that uses `core:time` and shares a `@(behavior="reset")` name with `Bank_Account`!
		file_2 :: `
package banking

import "core:time"

deposit :: proc(self: ^Bank_Account, amount: i64) {
	self.balance += amount
}

@(behavior="reset")
account_reset :: proc(self: ^Bank_Account) {
	self.balance = 0
}

@(actor)
Audit_Timer :: struct {
	interval: time.Duration,
}

set_interval :: proc(self: ^Audit_Timer, d: time.Duration) {
	self.interval = d
}

@(behavior="reset")
timer_reset :: proc(self: ^Audit_Timer) {
	self.interval = 0
}
`

		generated, ok := codegen.generate_from_sources({file_1, file_2}, {"account.odin", "timer.odin"})
		defer delete(generated)

		testing.expect(t, ok)
		// 1. Cross-file `import "core:time"` is automatically forwarded because `time.Duration` is used
		testing.expect(t, strings.contains(generated, `import "core:time"`))
		// 2. `Bank_Account` in File 1 matched `deposit` and `account_reset` from File 2
		testing.expect(t, strings.contains(generated, "Bank_Account_Msg_deposit :: struct"))
		testing.expect(t, strings.contains(generated, "Bank_Account_Msg_reset :: struct {}"))
		// 3. Both actors' `reset` behaviors are merged into a single `send_reset` overload group!
		testing.expect(t, strings.contains(generated, "send_bank_account_reset_actor"))
		testing.expect(t, strings.contains(generated, "send_audit_timer_reset_actor"))
		testing.expect(t, strings.contains(generated, "send_reset :: proc {"))
	})
}

@(test)
test_03_ast_codegen_ctx_behaviors_and_private_no_behavior_exclusions :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sample_src :: `
package worker_pkg

import "../actor"

@(actor)
Worker_Node :: struct {
	processed: int,
}

// 1. Behavior taking explicit (ctx: ^actor.Context, self: ^Worker_Node, ...) with a default parameter
step_with_ctx :: proc(ctx: ^actor.Context, self: ^Worker_Node, delta: int = 1) -> int {
	self.processed += delta
	return self.processed
}

// 2. Helper marked @(no_behavior) — MUST NOT generate a message struct or dispatch case!
@(no_behavior)
internal_helper :: proc(self: ^Worker_Node, x: int) {
	self.processed += x
}

// 3. Helper marked @(private) — MUST NOT generate a message struct or dispatch case!
@(private)
private_helper :: proc(self: ^Worker_Node) {
	self.processed = 0
}

// 4. Procedure for a non-actor struct — MUST be ignored and freed cleanly without leaking!
Other_Struct :: struct { a: int }
unrelated_proc :: proc(self: ^Other_Struct, v: int) {}
`

		generated, ok := codegen.generate_from_source(sample_src, "worker.odin")
		defer delete(generated)

		testing.expect(t, ok)
		// Explicit `ctx` is passed as first argument in dispatcher:
		testing.expect(t, strings.contains(generated, "res := step_with_ctx(ctx, self, m.delta)"))
		testing.expect(t, strings.contains(generated, "actor.reply(ctx, res)"))
		// `call_*` retains `delta: int = 1` (`param_sig`), whereas `send_*_reply_*` strips the default (`no_def_sig`)
		// so `delta: int` can precede `reply_to: actor.PID(int)`:
		testing.expect(t, strings.contains(generated, "call_worker_node_step_with_ctx_actor :: #force_inline proc(target: Worker_Node_Actor, delta: int = 1) -> int"))
		testing.expect(t, strings.contains(generated, "call_worker_node_step_with_ctx_pid :: #force_inline proc(target: actor.PID(Worker_Node_Msg), delta: int = 1) -> int"))
		testing.expect(t, strings.contains(generated, "send_worker_node_step_with_ctx_reply_actor :: #force_inline proc(target: Worker_Node_Actor, delta: int, reply_to: actor.PID(int))"))
		testing.expect(t, strings.contains(generated, "send_worker_node_step_with_ctx_reply_pid :: #force_inline proc(target: actor.PID(Worker_Node_Msg), delta: int, reply_to: actor.PID(int))"))
		// Excluded helpers and non-actor procedures are NOT present:
		testing.expect(t, !strings.contains(generated, "internal_helper"))
		testing.expect(t, !strings.contains(generated, "private_helper"))
		testing.expect(t, !strings.contains(generated, "Other_Struct"))
		testing.expect(t, !strings.contains(generated, "unrelated_proc"))
	})
}

