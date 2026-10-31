package main

@(actor)
Bank_Account :: struct {
	owner:    string,
	balance:  i64,
	tx_count: int,
}

// 1-way fire-and-forget behavior (`account->deposit(...)` / `send_deposit(account, ...)`)
deposit :: proc(self: ^Bank_Account, amount: i64, memo: string = "deposit") {
	self.balance  += amount
	self.tx_count += 1
}

// 1-way fire-and-forget behavior (`account->withdraw(...)` / `send_withdraw(account, ...)`)
withdraw :: proc(self: ^Bank_Account, amount: i64) {
	self.balance  -= amount
	self.tx_count += 1
}

// Request/Reply behavior returning `i64` (`account->get_balance()` / `call_get_balance(account)`)
get_balance :: proc(self: ^Bank_Account) -> i64 {
	return self.balance
}

@(behavior="reset")
account_reset :: proc(self: ^Bank_Account) {
	self.balance  = 0
	self.tx_count = 0
}

audit_snapshot :: proc(self: ^Bank_Account, logger: Audit_Logger_Actor) {
	// Call `Audit_Logger` using `->` method syntax (or `send_record_snapshot(logger, ...)`)
	logger->record_snapshot(self.owner, self.balance, self.tx_count)
}
