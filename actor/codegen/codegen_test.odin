package actor_codegen

import "core:strings"
import "core:testing"

@(test)
test_01_ast_codegen_parses_actor_and_behaviors :: proc(t: ^testing.T) {
	sample_src :: `
package demo

import "../actor"

@(actor)
Bank_Account :: struct {
	owner:   string,
	balance: i64,
}

@(behavior)
deposit :: proc(self: ^Bank_Account, amount: i64, memo: string = "cash") {
	self.balance += amount
}

@(behavior)
reset_balance :: proc(self: ^Bank_Account) {
	self.balance = 0
}

@(behavior)
query_balance :: proc(ctx: ^actor.Context, self: ^Bank_Account, reply_to: actor.PID(i64)) {
	actor.send(ctx, reply_to, self.balance)
}
`

	generated, ok := generate_from_source(sample_src, "bank.odin")
	defer delete(generated)

	testing.expect(t, ok)
	testing.expect(t, strings.contains(generated, "package demo"))
	testing.expect(t, strings.contains(generated, "Bank_Account_Msg_deposit :: struct"))
	testing.expect(t, strings.contains(generated, "Bank_Account_Msg_reset_balance :: struct {}"))
	testing.expect(t, strings.contains(generated, "Bank_Account_Msg :: union"))
	testing.expect(t, strings.contains(generated, "Bank_Account_Actor :: actor.Actor(Bank_Account, Bank_Account_Msg)"))
	testing.expect(t, strings.contains(generated, "Bank_Account_PID   :: actor.PID(Bank_Account_Msg)"))
	testing.expect(t, strings.contains(generated, "bank_account_dispatch :: proc(ctx: ^actor.Context, self: ^Bank_Account, msg: ^Bank_Account_Msg)"))
	testing.expect(t, strings.contains(generated, "deposit(self, m.amount, m.memo)"))
	testing.expect(t, strings.contains(generated, "reset_balance(self)"))
	testing.expect(t, strings.contains(generated, "query_balance(ctx, self, m.reply_to)"))
	testing.expect(t, strings.contains(generated, "spawn_bank_account :: proc"))
	testing.expect(t, strings.contains(generated, "send_deposit_actor :: #force_inline proc(target: ^Bank_Account_Actor, amount: i64, memo: string = \"cash\")"))
	testing.expect(t, strings.contains(generated, "be_deposit :: send_deposit"))
	testing.expect(t, strings.contains(generated, "be_reset_balance :: send_reset_balance"))
	testing.expect(t, strings.contains(generated, "be_query_balance :: send_query_balance"))
}
