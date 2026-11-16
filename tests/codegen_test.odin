package sleipnir_tests

import "core:os"
import "core:path/filepath"
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
		testing.expect(t, strings.contains(generated, "Bank_Account_PID :: actor.PID(Bank_Account_Msg)"))
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
		testing.expect(t, strings.contains(generated, "send_bank_account_get_balance_reply_pid :: #force_inline proc(target: actor.PID(Bank_Account_Msg), reply_to: actor.PID(i64))"))
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
		verify_ast_codegen_hardening_edge_cases(t)
	})
}

@(private = "file")
verify_ast_codegen_hardening_edge_cases :: proc(t: ^testing.T) {
		src_main :: `
package edge_pkg

import act "../actor"
import "core:math"
import "core:time"

@(actor)
Router :: struct {
	count: int,
}

@(behavior=` + "`custom_tick`" + `)
route_msg :: proc(ctx: ^act.Context, self: ^Router, target: string, reply_to: int, _: bool, aftermath := 1, memo := "ok", wait_dur: time.Duration) -> bool {
	return len(target) > 0 && reply_to >= 0 && aftermath > 0 && len(memo) > 0 && wait_dur >= 0
}

// Polymorphic behavior MUST be skipped safely:
poly_helper :: proc(self: ^Router, x: $T) {}
`
		src_file_private :: `
#+private file
package edge_pkg

file_local_helper :: proc(self: ^Router, secret: int) {}
`

		generated, ok := codegen.generate_from_sources(
			[]string{src_main, src_file_private},
			[]string{"router.odin", "router_priv.odin"},
		)
		defer delete(generated)

		testing.expect(t, ok)
		testing.expect(t, strings.contains(generated, "import act \"../actor\""))
		testing.expect(t, strings.contains(generated, "import \"core:time\""))
		// Token-boundary check: `aftermath` identifier must NOT pull in `"core:math"`
		testing.expect(t, !strings.contains(generated, "\"core:math\""))
		// Aliased `act` import must be respected in generated types and calls
		testing.expect(t, strings.contains(generated, "Router_PID :: act.PID(Router_Msg)"))
		testing.expect(t, strings.contains(generated, "router_dispatch :: proc(ctx: ^act.Context, self: ^Router, msg: ^Router_Msg)"))
		testing.expect(t, strings.contains(generated, "act.reply(ctx, res)"))
		// Backtick attribute `@(behavior=`custom_tick`)`
		testing.expect(t, strings.contains(generated, "Router_Msg_custom_tick :: struct {"))
		// Blank `_` parameter synthesized as `_arg0` and type-inferred defaults (`:=`) given concrete types
		testing.expect(t, strings.contains(generated, "_arg0: bool,"))
		testing.expect(t, strings.contains(generated, "aftermath: int,"))
		testing.expect(t, strings.contains(generated, "memo: string,"))
		// Parameter collision disambiguation (`target` / `reply_to` in params -> `_sleipnir_target` / `_sleipnir_reply_to`)
		testing.expect(t, strings.contains(generated, "call_router_custom_tick_actor :: #force_inline proc(_sleipnir_target: Router_Actor, target: string, reply_to: int, _arg0: bool, aftermath: int = 1, memo: string = \"ok\", wait_dur: time.Duration) -> bool"))
		testing.expect(t, strings.contains(generated, "send_router_custom_tick_reply_pid :: #force_inline proc(_sleipnir_target: act.PID(Router_Msg), target: string, reply_to: int, _arg0: bool, aftermath: int, memo: string, wait_dur: time.Duration, _sleipnir_reply_to: act.PID(bool))"))
		testing.expect(t, strings.contains(generated, "Router_Msg(Router_Msg_custom_tick{target = target, reply_to = reply_to, _arg0 = _arg0, aftermath = aftermath, memo = memo, wait_dur = wait_dur})"))
		testing.expect(t, strings.contains(generated, "res := route_msg(ctx, self, m.target, m.reply_to, m._arg0, m.aftermath, m.memo, m.wait_dur)"))
		// Polymorphic `$T` and `#+private file` procedures MUST be excluded
		testing.expect(t, !strings.contains(generated, "poly_helper"))
		testing.expect(t, !strings.contains(generated, "file_local_helper"))

		// Verify `generate_directory` ignores `*_test.odin` files
		tmp_dir := ".tmp_codegen_dir_test"
		_ = os.make_directory(tmp_dir)
		main_file, _ := filepath.join({tmp_dir, "ping.odin"}, context.temp_allocator)
		test_file, _ := filepath.join({tmp_dir, "ping_test.odin"}, context.temp_allocator)
		ping_src :: `package ping_pkg
import "../actor"
@(actor)
Ping :: struct { count: int }
ping :: proc(self: ^Ping) { self.count += 1 }
`
		ping_test_src :: `package ping_pkg
import "core:testing"
test_helper :: proc(self: ^Ping) {}
@(test)
test_ping :: proc(t: ^testing.T) {}
`
		_ = os.write_entire_file(main_file, transmute([]byte)string(ping_src))
		_ = os.write_entire_file(test_file, transmute([]byte)string(ping_test_src))
		defer {
			_ = os.remove(main_file)
			_ = os.remove(test_file)
			gen_file, _ := filepath.join({tmp_dir, "actors_gen.odin"}, context.temp_allocator)
			_ = os.remove(gen_file)
			_ = os.remove(tmp_dir)
		}

		out_path, dir_ok := codegen.generate_directory(tmp_dir)
		defer {
			if dir_ok {
				delete(out_path)
			}
		}
		testing.expect(t, dir_ok)
		if dir_ok {
			gen_bytes, rerr := os.read_entire_file(out_path, context.allocator)
			testing.expect(t, rerr == nil)
			if rerr == nil {
				defer delete(gen_bytes)
				gen_str := string(gen_bytes)
				testing.expect(t, strings.contains(gen_str, "Ping_Msg_ping"))
				testing.expect(t, !strings.contains(gen_str, "test_helper"))
				testing.expect(t, !strings.contains(gen_str, "core:testing"))
			}
		}
}

@(test)
test_04_ast_codegen_supervise_and_reinit_attributes :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		sample_src :: `
package demo

import "../actor"

@(actor, supervise="strategy=one_for_one,restart=Transient,use_arena=true")
Org_Router :: struct {
	org_id:    u64,
	committed: i64,
}

@(reinit)
org_router_reinit :: proc(self: ^Org_Router, alloc: actor.Allocator) {
	self.committed = 42
}

@(behavior)
route :: proc(self: ^Org_Router, x: i64) -> i64 {
	return self.committed + x
}

@(actor)
Stateless_Worker :: struct {
	count: int,
}

@(reinit)
worker_reinit :: proc(self: ^Stateless_Worker) {
	self.count = 0
}
`
		generated, ok := codegen.generate_from_source(sample_src, "supervise_sample.odin")
		defer delete(generated)

		testing.expect(t, ok)
		testing.expect(t, !strings.contains(generated, "_Msg_org_router_reinit"))
		testing.expect(t, !strings.contains(generated, "_Msg_worker_reinit"))
		testing.expect(t, strings.contains(generated, "Org_Router_PID :: actor.PID(Org_Router_Msg)"))
		testing.expect(t, strings.contains(generated, "Stateless_Worker_PID :: actor.PID(Stateless_Worker_Msg)"))
		testing.expect(t, strings.contains(generated, "_reinit_trampoline"))
		testing.expect(t, strings.contains(generated, "Org_Router_DEFAULT_RESTART_POLICY :: actor.Restart_Policy.Transient"))
		testing.expect(t, strings.contains(generated, "spawn_org_router_sup"))
		testing.expect(t, strings.contains(generated, "spawn_stateless_worker_sup"))
	})
}

@(test)
test_05_codegen_rejects_invalid_supervise_and_reinit_attributes :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		Invalid_Case :: struct {
			name: string,
			src:  string,
		}

		cases := [5]Invalid_Case{
			{
				name = "unknown_supervise_strategy",
				src  = `package bad1
import "../actor"
@(actor, supervise="strategy=unknown_mode")
Bad_Actor :: struct { x: int }
`,
			},
			{
				name = "negative_max_restarts",
				src  = `package bad2
import "../actor"
@(actor, supervise="max_restarts=-1")
Bad_Actor :: struct { x: int }
`,
			},
			{
				name = "duplicate_reinit_for_same_actor",
				src  = `package bad3
import "../actor"
@(actor)
Dup_Actor :: struct { x: int }
@(reinit)
reinit_a :: proc(self: ^Dup_Actor) {}
@(reinit)
reinit_b :: proc(self: ^Dup_Actor, alloc: actor.Allocator) {}
`,
			},
			{
				name = "reinit_mismatched_receiver_pointer",
				src  = `package bad4
import "../actor"
@(actor)
Target_Actor :: struct { x: int }
Other_Data :: struct { y: int }
@(reinit)
bad_reinit :: proc(self: ^Other_Data) {}
`,
			},
			{
				name = "behavior_and_reinit_mutually_exclusive",
				src  = `package bad5
import "../actor"
@(actor)
Mixed_Actor :: struct { x: int }
@(behavior, reinit)
both_handler :: proc(self: ^Mixed_Actor) {}
`,
			},
		}

		for tc in cases {
			diags, ok := codegen.validate_source(tc.src, "invalid_supervise.odin")
			defer delete(diags)

			testing.expectf(t, !ok, "%s should fail validation", tc.name)
			testing.expectf(t, len(diags) > 0, "%s should produce diagnostics", tc.name)
			if len(diags) > 0 {
				testing.expectf(t, diags[0].line > 0, "%s diagnostic should report positive line number", tc.name)
			}
		}
	})
}

@(test)
test_06_codegen_warns_or_rejects_ambiguous_rpc_reply_signature :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		Ambiguous_Case :: struct {
			name: string,
			src:  string,
		}

		cases := [2]Ambiguous_Case{
			{
				name = "conflicting_reply_to_and_return_type",
				src  = `package rpc_bad1
import "../actor"
@(actor)
Rpc_Worker :: struct { val: int }
@(behavior)
compute :: proc(self: ^Rpc_Worker, x: int, reply_to: actor.PID(int)) -> int {
	return x + self.val
}
`,
			},
			{
				name = "malformed_reply_channel_type",
				src  = `package rpc_bad2
import "../actor"
@(actor)
Rpc_Worker :: struct { val: int }
@(behavior)
compute :: proc(self: ^Rpc_Worker, x: int, reply_to: actor.Reply($T)) {
	_ = x
}
`,
			},
		}

		for tc in cases {
			generated, _ := codegen.generate_from_source(tc.src, "ambiguous_rpc.odin")
			delete(generated)

			diags, ok := codegen.validate_source(tc.src, "ambiguous_rpc.odin")
			defer delete(diags)

			testing.expectf(t, !ok, "%s should fail validation cleanly without crashing", tc.name)
			testing.expectf(t, len(diags) > 0, "%s should report an explicit RPC diagnostic", tc.name)
		}
	})
}

@(test)
test_07_codegen_regression_multi_return_grouped_receiver_alias_and_guards :: proc(t: ^testing.T) {
	with_leak_check(t, proc(t: ^testing.T) {
		file_a :: `
package edge_reg

import act "../actor"

@(actor, supervise="restart=Permanent")
Node :: struct {
	val: int,
}

pair_status :: proc(self: ^Node, res: int) -> (int, bool) {
	return self.val + res, true
}
`
		file_b :: `
package edge_reg

import a "../actor"

link_peer :: proc(ctx: ^a.Context, self, peer: ^Node, act: a.PID(int)) {
	_ = ctx
	_ = peer
	_ = act
}
`

		generated, ok := codegen.generate_from_sources({file_a, file_b}, {"a.odin", "b.odin"})
		defer delete(generated)

		testing.expect(t, ok)
		// BUG-004: unnamed multi-return `(int, bool)` generates 2-field result struct
		testing.expect(t, strings.contains(generated, "Node_pair_status_Result :: struct {"))
		testing.expect(t, strings.contains(generated, "\tr0: int,"))
		testing.expect(t, strings.contains(generated, "\tr1: bool,"))
		testing.expect(t, strings.contains(generated, ") -> (int, bool)"))
		// BUG-021: `_sleipnir_res` prevents collision with parameter named `res`, and `act` param is disambiguated
		testing.expect(t, strings.contains(generated, "_sleipnir_res := act.call("))
		testing.expect(t, strings.contains(generated, "_sleipnir_param_act: act.PID(int)"))
		// BUG-020 & BUG-026: grouped `self, peer: ^Node` emits `peer: ^Node`, and `a.` normalizes to `act.`
		testing.expect(t, strings.contains(generated, "\tpeer: ^Node,"))
		testing.expect(t, strings.contains(generated, "\tact: act.PID(int),"))
		// BUG-027: `spawn_*_sup` guards `sup == nil` and `a == nil`
		testing.expect(t, strings.contains(generated, "if sup == nil {"))
		testing.expect(t, strings.contains(generated, "if a == nil {"))
	})
}

