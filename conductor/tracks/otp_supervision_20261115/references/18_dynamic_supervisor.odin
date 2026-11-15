// =============================================================================
// 5. Multi-Tenant `Dynamic_Supervisor` (Keyed by `org_id: u64`)
// =============================================================================
MAX_TENANT_ACTORS :: 8
// Returned by your tenant factory callback when an `org_id` is spawned on demand.
Tenant_Subtree :: struct {
    sup:      ^Actor(Supervisor_State, Supervisor_Msg), // Per-org supervisor (e.g. `.Rest_For_One`)
    router:   ^Actor_Base,                              // Entry-point actor for `org_id`
    msg_type: typeid,                                   // Message type accepted by `router`
    children: [dynamic; MAX_TENANT_ACTORS]^Actor_Base,  // Child actors owned by this tenant
    user_ctx: rawptr,                                   // Optional per-tenant context (e.g. xitdb handle)
}
Tenant_Factory_Proc :: #type proc(sched: ^Scheduler, org_id: u64) -> Tenant_Subtree
Tenant_Evict_Proc   :: #type proc(org_id: u64, subtree: ^Tenant_Subtree)
@(private)
Tenant_Record :: struct {
    org_id:      u64,
    last_active: time.Tick,
    subtree:     Tenant_Subtree,
}
Dynamic_Supervisor :: struct {
    mu:            sync.RW_Mutex,
    sched:         ^Scheduler,
    idle_timeout:  time.Duration,
    factory:       Tenant_Factory_Proc,
    on_evict:      Tenant_Evict_Proc,
    tenants:       map[u64]^Tenant_Record,
    registry:      ^Registry(u64), // Optional shared registry auto-updated on spawn/evict
    evicted_count: int,
    allocator:     mem.Allocator,
}
dynamic_supervisor_init :: proc(
    ds: ^Dynamic_Supervisor,
    factory: Tenant_Factory_Proc,
    idle_timeout: time.Duration = 15 * time.Minute,
    on_evict: Tenant_Evict_Proc = nil,
    registry: ^Registry(u64) = nil,
    s: ^Scheduler = nil,
    allocator := context.allocator,
) {
    ds.sched         = s if s != nil else current_scheduler()
    ds.idle_timeout  = idle_timeout
    ds.factory       = factory
    ds.on_evict      = on_evict
    ds.registry      = registry
    ds.evicted_count = 0
    ds.allocator     = allocator
    ds.tenants       = make(map[u64]^Tenant_Record, 64, allocator)
}
@(private)
destroy_tenant_record :: proc(ds: ^Dynamic_Supervisor, rec: ^Tenant_Record) {
    // 1. Stop and destroy all child actors in the tenant's subtree
    for i := 0; i < len(rec.subtree.children); i += 1 {
        actor_base_destroy(rec.subtree.children[i])
    }
    // 2. Stop and destroy the tenant's per-org supervisor actor
    if rec.subtree.sup != nil {
        actor_base_destroy(&rec.subtree.sup.base)
    }
    free(rec, ds.allocator)
}
dynamic_supervisor_destroy :: proc(ds: ^Dynamic_Supervisor) {
    sync.rw_mutex_lock(&ds.mu)
    for _, rec in ds.tenants {
        if rec != nil {
            destroy_tenant_record(ds, rec)
        }
    }
    delete(ds.tenants)
    sync.rw_mutex_unlock(&ds.mu)
}
// Look up `org_id`'s router `PID($Msg)`, spawning its supervision subtree on demand
// if not currently active, and refreshing its `last_active` timestamp.
@(require_results)
get_or_spawn_tenant :: proc(
    ds: ^Dynamic_Supervisor,
    org_id: u64,
    $Msg: typeid,
) -> PID(Msg) {
    now := time.tick_now()
    // 1. Fast path: shared read lock if tenant is already running
    sync.rw_mutex_shared_lock(&ds.mu)
    if rec, found := ds.tenants[org_id]; found && rec != nil {
        intrinsics.atomic_store_explicit((^i64)(&rec.last_active._nsec), now._nsec, .Relaxed)
        router := rec.subtree.router
        sync.rw_mutex_shared_unlock(&ds.mu)
        return PID(Msg){ref = cast(^Actor_Ref(Msg))router}
    }
    sync.rw_mutex_shared_unlock(&ds.mu)
    // 2. Slow path: exclusive write lock to spawn the tenant's supervision tree
    sync.rw_mutex_lock(&ds.mu)
    defer sync.rw_mutex_unlock(&ds.mu)
    if rec, found := ds.tenants[org_id]; found && rec != nil {
        rec.last_active = now
        return PID(Msg){ref = cast(^Actor_Ref(Msg))rec.subtree.router}
    }
    rec := new(Tenant_Record, ds.allocator)
    rec.org_id      = org_id
    rec.last_active = now
    rec.subtree     = ds.factory(ds.sched, org_id)
    ds.tenants[org_id] = rec
    if ds.registry != nil && rec.subtree.router != nil {
        sync.rw_mutex_lock(&ds.registry.mu)
        ds.registry.entries[org_id] = Registry_Entry{
            ref      = rec.subtree.router,
            msg_type = rec.subtree.msg_type,
        }
        sync.rw_mutex_unlock(&ds.registry.mu)
    }
    return PID(Msg){ref = cast(^Actor_Ref(Msg))rec.subtree.router}
}
// Send a message directly to `org_id`'s router actor, spawning the tenant subtree if needed.
send_to_tenant :: proc(ds: ^Dynamic_Supervisor, org_id: u64, msg: $Msg) {
    router_pid := get_or_spawn_tenant(ds, org_id, Msg)
    send(ds.sched, router_pid, msg)
}
// Synchronous `actor.call` to `org_id`'s router actor, spawning the tenant subtree if needed.
@(require_results)
call_tenant :: proc(ds: ^Dynamic_Supervisor, org_id: u64, msg: $Msg, $Res: typeid) -> Res {
    router_pid := get_or_spawn_tenant(ds, org_id, Msg)
    return call(ds.sched, router_pid, msg, Res)
}
// Sweep all tenants whose `last_active` is older than `ds.idle_timeout` (or `custom_cutoff`).
// Runs `ds.on_evict` (e.g. to flush `xitdb` to Tigris), unregisters from `ds.registry`,
// and destroys all actors in the evicted tenant's supervision tree.
sweep_idle_tenants :: proc(ds: ^Dynamic_Supervisor, now_override: time.Tick = {}) -> int {
    now := now_override if now_override._nsec != 0 else time.tick_now()
    cutoff := time.tick_add(now, -ds.idle_timeout)
    to_evict := make([dynamic]^Tenant_Record, 0, 8, context.temp_allocator)
    sync.rw_mutex_lock(&ds.mu)
    for org_id, rec in ds.tenants {
        if rec == nil {
            continue
        }
        last_nsec := intrinsics.atomic_load_explicit((^i64)(&rec.last_active._nsec), .Relaxed)
        last_tick := time.Tick{_nsec = last_nsec}
        if time.tick_diff(cutoff, last_tick) > 0 {
            append(&to_evict, rec)
            delete_key(&ds.tenants, org_id)
            if ds.registry != nil {
                registry_unregister(ds.registry, org_id)
            }
        }
    }
    ds.evicted_count += len(to_evict)
    sync.rw_mutex_unlock(&ds.mu)
    for rec in to_evict {
        if ds.on_evict != nil {
            ds.on_evict(rec.org_id, &rec.subtree)
        }
        destroy_tenant_record(ds, rec)
    }
    return len(to_evict)
}
