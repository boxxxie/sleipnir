package banking
import "../actor"
import "core:mem"
@(actor, supervise="Permanent")
Tenant_Ledger :: struct {
    org_id:  u64,
    balance: i64,
    scratch: []byte,
}
// Automatically wired into `actor.enable_actor_supervision` and excluded from message union!
// Supports either `proc(self: ^T)` or `proc(self: ^T, alloc: mem.Allocator)`.
@(reinit)
rehydrate_ledger :: proc(self: ^Tenant_Ledger, alloc: mem.Allocator) {
    // Called on initial spawn AND after every supervisor arena wipe + restart
    self.scratch = make([]byte, 1024, alloc)
}
deposit :: proc(self: ^Tenant_Ledger, amount: i64) {
    self.balance += amount
}
get_balance :: proc(self: ^Tenant_Ledger) -> i64 {
    return self.balance
}
