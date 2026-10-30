package main

import "../../actor"
import "core:fmt"

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

@(behavior="reset")
logger_reset :: proc(self: ^Audit_Logger) {
	self.entries_logged = 0
	self.last_balance   = 0
}
