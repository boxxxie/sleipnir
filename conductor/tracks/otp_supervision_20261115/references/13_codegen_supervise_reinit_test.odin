@(test)
test_04_ast_codegen_supervise_and_reinit_attributes :: proc(t: ^testing.T) {
    with_leak_check(t, proc(t: ^testing.T) {
        sample_src :: `
package tenant_pkg
import "../actor"
import "core:mem"
@(actor, supervise="Transient")
Org_Router :: struct {
    org_id:  u64,
    routes:  []u64,
    handled: int,
}
// 2-arg @(reinit) taking (self: ^Org_Router, alloc: mem.Allocator)
@(reinit)
org_router_reinit :: proc(self: ^Org_Router, alloc: mem.Allocator) {
    self.routes = make([]u64, 16, alloc)
}
route_packet :: proc(self: ^Org_Router, packet_id: u64) -> int {
    self.handled += 1
    return self.handled
}
@(actor)
 Stateless_Worker :: struct {
    count: int,
}
// 1-arg @(reinit) on an @(actor) without explicit @(supervise) still enables supervision!
@(reinit)
worker_reinit :: proc(self: ^Stateless_Worker) {
    self.count = 1
}
bump :: proc(self: ^Stateless_Worker) {
    self.count += 1
}
`
        generated, ok := codegen.generate_from_source(sample_src, "tenant.odin")
        defer delete(generated)
        testing.expect(t, ok)
        // 1. `@(reinit)` procedures MUST NOT become message structs or `send_*` callers:
        testing.expect(t, !strings.contains(generated, "Org_Router_Msg_org_router_reinit"))
        testing.expect(t, !strings.contains(generated, "Stateless_Worker_Msg_worker_reinit"))
        testing.expect(t, !strings.contains(generated, "send_org_router_reinit"))
        testing.expect(t, !strings.contains(generated, "send_worker_reinit"))
        // 2. 2-arg `org_router_reinit` forwards `(self, alloc)` and defaults policy to `.Transient`:
        testing.expect(t, strings.contains(generated, "org_router_reinit_hook :: proc(self: ^Org_Router,
        testing.expect(t, strings.contains(generated, "org_router_reinit(self, alloc)"))
        testing.expect(t, strings.contains(generated, "actor.enable_actor_supervision(a, initial_state, o
        testing.expect(t, strings.contains(generated, "policy: actor.Restart_Policy = .Transient,"))
        testing.expect(t, strings.contains(generated, "actor.supervise(sup, a, initial_state, policy, org
        testing.expect(t, strings.contains(generated, "spawn_org_router_sup,"))
        // 3. 1-arg `worker_reinit` ignores `alloc` cleanly and defaults policy to `.Permanent`:
        testing.expect(t, strings.contains(generated, "stateless_worker_reinit_hook :: proc(self: ^Statel
        testing.expect(t, strings.contains(generated, "worker_reinit(self)"))
        testing.expect(t, strings.contains(generated, "policy: actor.Restart_Policy = .Permanent,"))
        testing.expect(t, strings.contains(generated, "spawn_stateless_worker_sup,"))
    })
}
