package main

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

@(behavior="reset")
account_reset :: proc(self: ^Bank_Account) {
	self.balance  = 0
	self.tx_count = 0
}

@(behavior)
audit_snapshot :: proc(self: ^Bank_Account, logger: Audit_Logger_PID) {
	// Call a Pony-style behavior on `Audit_Logger` (defined in `audit_logger.odin`)!
	be_record_snapshot(logger, self.owner, self.balance, self.tx_count)
}
