// Add to Layer 1 in tests/supervision_test.odin:
@(test)
test_22_dynamic_supervisor_on_demand_spawn_registry_and_idle_eviction :: proc(t: ^testing.T) {
    with_leak_check(t, proc(t: ^testing.T) {
        w := supervision_world(t, actor.scoped_scheduler(num_workers = 2))
        defer destroy_dynamic_supervision_world(&w)
        given_multi_tenant_dynamic_supervisor(&w, idle_timeout = 50 * time.Millisecond)
        // 1. Send traffic to Org 101 and Org 102 — spawns two isolated `.Rest_For_One` trees on demand:
        upon_sending_to_org(&w, org_id = 101, msg = Tenant_Add{delta = 40, scratch_bytes = 512})
        upon_sending_to_org(&w, org_id = 102, msg = Tenant_Add{delta = 90, scratch_bytes = 512})
        then_org_is_registered(&w, org_id = 101, expected = true)
        then_org_is_registered(&w, org_id = 102, expected = true)
        then_org_balance_is(&w, org_id = 101, expected = 140) // 100 initial + 40
        then_org_balance_is(&w, org_id = 102, expected = 190) // 100 initial + 90
        // 2. Crash Org 101's router with an out-of-bounds slice access; Org 101 recovers to 100,
        //    while Org 102 is completely unaffected at 190:
        upon_sending_to_org(&w, org_id = 101, msg = Tenant_Force_Bounds_Panic{bad_index = 77})
        then_org_balance_is(&w, org_id = 101, expected = 100)
        then_org_balance_is(&w, org_id = 102, expected = 190)
        // 3. Advance simulated clock past 50ms idle window for Org 101 while keeping Org 102 active:
        upon_sweeping_orgs_after_idle(&w, active_org_id = 102, advance = 100 * time.Millisecond)
        then_evicted_orgs_are(&w, expected_count = 1, last_evicted_org = 101)
        then_org_is_registered(&w, org_id = 101, expected = false)
        then_org_is_registered(&w, org_id = 102, expected = true)
        then_org_balance_is(&w, org_id = 102, expected = 200) // 190 + 10 touch
    })
}
