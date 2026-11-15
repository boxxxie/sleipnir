// Add to Supervision_World in tests/supervision_test.odin:
//   reg:              actor.Registry(u64),
//   dyn_sup:          actor.Dynamic_Supervisor,
//   has_dyn_sup:      bool,
//   last_evicted_org: u64,
@(private = "file")
g_last_evicted_org: u64
@(private = "file")
destroy_dynamic_supervision_world :: proc(w: ^Supervision_World) {
    if w.has_dyn_sup {
        actor.wait_idle(w.sched)
        actor.dynamic_supervisor_destroy(&w.dyn_sup)
        actor.registry_destroy(&w.reg)
        w.has_dyn_sup = false
    }
}
@(private = "file")
given_multi_tenant_dynamic_supervisor :: proc(w: ^Supervision_World, idle_timeout: time.Duration) {
    g_last_evicted_org = 0
    actor.registry_init(&w.reg)
    actor.dynamic_supervisor_init(
        ds           = &w.dyn_sup,
        idle_timeout = idle_timeout,
        registry     = &w.reg,
        s            = w.sched,
        factory      = proc(sched: ^actor.Scheduler, org_id: u64) -> actor.Tenant_Subtree {
            // Per-org `.Rest_For_One` supervisor + xitdb owner + router actor
            org_sup := actor.spawn_supervisor(
                strategy      = .Rest_For_One,
                max_restarts  = 5,
                within_window = 10 * time.Second,
                s             = sched,
            )
            init_db     := Tenant_State{total = 100}
            init_router := Tenant_State{total = 100}
            db_owner := actor.spawn(sched, init_db, tenant_behavior, id = u32(org_id * 10 + 1))
            router   := actor.spawn(sched, init_router, tenant_behavior, id = u32(org_id * 10 + 2))
            actor.supervise(org_sup, db_owner, init_db, .Permanent, use_arena = true, keep_mailbox = fa
            actor.supervise(org_sup, router, init_router, .Permanent, use_arena = true, keep_mailbox =
            subtree := actor.Tenant_Subtree{
                sup      = org_sup,
                router   = &router.base,
                msg_type = typeid_of(Tenant_Msg),
            }
            append(&subtree.children, &db_owner.base)
            append(&subtree.children, &router.base)
            return subtree
        },
        on_evict = proc(org_id: u64, subtree: ^actor.Tenant_Subtree) {
            _ = subtree
            g_last_evicted_org = org_id
        },
    )
    w.has_dyn_sup = true
}
@(private = "file")
upon_sending_to_org :: proc(w: ^Supervision_World, org_id: u64, msg: Tenant_Msg) {
    actor.send_to_tenant(&w.dyn_sup, org_id, msg)
    actor.wait_idle(w.sched)
}
@(private = "file")
upon_sweeping_orgs_after_idle :: proc(
    w: ^Supervision_World,
    active_org_id: u64,
    advance: time.Duration,
) {
    future_tick := time.tick_add(time.tick_now(), advance)
    // Refresh `active_org_id` at `future_tick` so only idle orgs are evicted:
    actor.send_to_tenant(&w.dyn_sup, active_org_id, Tenant_Msg(Tenant_Add{delta = 10}))
    actor.wait_idle(w.sched)
    if rec, ok := w.dyn_sup.tenants[active_org_id]; ok && rec != nil {
        rec.last_active = future_tick
    }
    _ = actor.sweep_idle_tenants(&w.dyn_sup, now_override = future_tick)
}
@(private = "file")
then_org_is_registered :: proc(
    w: ^Supervision_World,
    org_id: u64,
    expected: bool,
    loc := #caller_location,
) {
    _, found := actor.registry_lookup(&w.reg, org_id, Tenant_Msg)
    testing.expect_value(w.t, found, expected, loc = loc)
}
@(private = "file")
then_org_balance_is :: proc(
    w: ^Supervision_World,
    org_id: u64,
    expected: i64,
    loc := #caller_location,
) {
    // Wait briefly if the router is mid-restart after a simulated panic
    actual: i64
    for _ in 0 ..< 1000 {
        actor.wait_idle(w.sched)
        actual = actor.call_tenant(&w.dyn_sup, org_id, Tenant_Msg(Tenant_Get_Total{}), i64)
        if actual == expected {
            break
        }
        time.sleep(100 * time.Microsecond)
    }
    testing.expect_value(w.t, actual, expected, loc = loc)
}
@(private = "file")
then_evicted_orgs_are :: proc(
    w: ^Supervision_World,
    expected_count: int,
    last_evicted_org: u64,
    loc := #caller_location,
) {
    testing.expect_value(w.t, w.dyn_sup.evicted_count, expected_count, loc = loc)
    testing.expect_value(w.t, g_last_evicted_org, last_evicted_org, loc = loc)
}
