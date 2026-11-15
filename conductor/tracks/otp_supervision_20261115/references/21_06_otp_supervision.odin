// =============================================================================
// Example 06: Multi-Tenant OTP Supervision Tree (`Dynamic_Supervisor` + `.Rest_For_One`)
//
// Demonstrates:
//   1. `Dynamic_Supervisor` spawning isolated per-organization (`.Rest_For_One`)
//      supervision trees on demand for `Org_101` and `Org_102`.
//   2. Per-actor `vmem.Arena` (`context.allocator` inside handlers) + `setjmp`/`longjmp`
//      panic recovery via `context.assertion_failure_proc`.
//   3. Atomic state rehydration from a simulated `xitdb` committed moment when an
//      actor panics mid-turn (e.g. out-of-bounds slice access), without dropping
//      the actor's `PID` or affecting peer organizations.
//   4. `.Rest_For_One` cascading restart when `Org_101`'s `Xitdb_Owner` crashes.
//   5. Idle-timeout eviction (`sweep_idle_tenants`) flushing `Org_101` and freeing
//      its entire supervision subtree with zero memory leaks (`Tracking_Allocator`).
//
// Run:
//   odin run examples/06_otp_supervision.odin -file -o:speed
// =============================================================================
package main
import "../actor"
import "core:fmt"
import "core:mem"
import "core:time"
// -----------------------------------------------------------------------------
// 1. Simulated Durable `xitdb` Moment Store (Shared Per-Org Handle)
// -----------------------------------------------------------------------------
Xitdb_Slot :: struct {
    org_id:           u64,
    committed_moment: i64,
    commits:          int,
    flushed_to_cloud: bool,
}
// -----------------------------------------------------------------------------
// 2. Child 0: `Xitdb_Owner` Actor (Owns the Organization's Durable Store)
// -----------------------------------------------------------------------------
Db_Commit :: struct {
    delta: i64,
}
Db_Simulate_Crash :: struct {}
Db_Get_Moment :: struct {}
Db_Msg :: union {
    Db_Commit,
    Db_Simulate_Crash,
    Db_Get_Moment,
}
Xitdb_Owner_State :: struct {
    org_id:  u64,
    slot:    ^Xitdb_Slot,
    handles: int,
}
xitdb_owner_receive :: proc(self: ^Xitdb_Owner_State, msg: Db_Msg) {
    switch m in msg {
    case Db_Commit:
        self.slot.committed_moment += m.delta
        self.slot.commits += 1
        actor.reply(self.slot.committed_moment)
    case Db_Simulate_Crash:
        fmt.printfln("  [Org_%d Xitdb_Owner] Simulating storage handle fault!", self.org_id)
        actor.fail_turn(.Invalid_State, "xitdb handle invalidated")
    case Db_Get_Moment:
        actor.reply(self.slot.committed_moment)
    }
}
// -----------------------------------------------------------------------------
// 3. Child 2: `Bot_Worker` Actor (Executes Turn Tasks Downstream of Router)
// -----------------------------------------------------------------------------
Bot_Task :: struct {
    turn_id: int,
    tokens:  i64,
}
Bot_Worker_State :: struct {
    org_id:         u64,
    turns_executed: int,
    tokens_used:    i64,
}
bot_worker_receive :: proc(self: ^Bot_Worker_State, task: Bot_Task) {
    self.turns_executed += 1
    self.tokens_used += task.tokens
}
// -----------------------------------------------------------------------------
// 4. Child 1: `Org_Router` Actor (Stateful Router with Per-Actor `vmem.Arena`)
// -----------------------------------------------------------------------------
Route_Turn :: struct {
    delta:         i64,
    scratch_bytes: int,
}
Route_Malformed_Payload :: struct {
    oob_index: int,
}
Route_Crash_Db :: struct {}
Route_Query_Status :: struct {}
Router_Status :: struct {
    org_id:     u64,
    live_value: i64,
    db_gen:     u32,
    router_gen: u32,
    bot_gen:    u32,
}
Router_Msg :: union {
    Route_Turn,
    Route_Malformed_Payload,
    Route_Crash_Db,
    Route_Query_Status,
}
Org_Router_State :: struct {
    org_id:       u64,
    slot:         ^Xitdb_Slot,
    db_actor:     ^actor.Actor(Xitdb_Owner_State, Db_Msg),
    bot_actor:    ^actor.Actor(Bot_Worker_State, Bot_Task),
    live_value:   i64,
    turns:        int,
    turn_scratch: []byte, // Allocated inside this actor's `vmem.Arena`
}
// Called on initial spawn AND automatically after every crash + O(1) `vmem.Arena` wipe:
org_router_reinit :: proc(self: ^Org_Router_State, arena_alloc: mem.Allocator) {
    // Rehydrate living state from the last committed `xitdb` moment
    self.live_value   = self.slot.committed_moment
    self.turn_scratch = make([]byte, 256, arena_alloc)
}
org_router_receive :: proc(self: ^Org_Router_State, msg: Router_Msg) {
    switch m in msg {
    case Route_Turn:
        if m.scratch_bytes > 0 {
            // Allocated directly inside `self`'s `vmem.Arena` (`context.allocator`)
            self.turn_scratch = make([]byte, m.scratch_bytes)
        }
        self.turns += 1
        // Commit durable state change to `Xitdb_Owner` and forward work to `Bot_Worker`
        self.live_value = actor.call(self.db_actor, Db_Msg(Db_Commit{delta = m.delta}), i64)
        actor.send(self.bot_actor, Bot_Task{turn_id = self.turns, tokens = m.delta * 10})
    case Route_Malformed_Payload:
        // 1. Mutate in-memory state and allocate 16 KiB in the actor's `vmem.Arena`
        //    WITHOUT committing a new `xitdb` moment yet:
        self.live_value = -999_999
        self.turn_scratch = make([]byte, 16 * 1024)
        // 2. Trigger a real Odin out-of-bounds slice check (`context.assertion_failure_proc`)
        fmt.printfln("  [Org_%d Router] Processing malformed payload (index %d)...", self.org_id, m.oob_i
        headers := [2]u8{0xAA, 0xBB}
        _ = headers[m.oob_index] // Panics -> trapped by Sleipnir worker -> O(1) arena wipe!
    case Route_Crash_Db:
        actor.send(self.db_actor, Db_Msg(Db_Simulate_Crash{}))
    case Route_Query_Status:
        actor.reply(Router_Status{
            org_id     = self.org_id,
            live_value = self.live_value,
            db_gen     = self.db_actor.base.generation,
            router_gen = self.db_actor.base.generation, // Replaced below in factory wrapper if needed
            bot_gen    = self.bot_actor.base.generation,
        })
    }
}
// -----------------------------------------------------------------------------
// 5. Multi-Tenant Supervision Tree Demo
// -----------------------------------------------------------------------------
g_org_slots: [2]Xitdb_Slot
slot_for_org :: proc(org_id: u64) -> ^Xitdb_Slot {
    idx := 0 if org_id == 101 else 1
    g_org_slots[idx].org_id = org_id
    return &g_org_slots[idx]
}
wait_for_restarts :: proc(
    sched: ^actor.Scheduler,
    sup: ^actor.Actor(actor.Supervisor_State, actor.Supervisor_Msg),
    expected: int,
) -> actor.Supervisor_Stats {
    stats: actor.Supervisor_Stats
    for _ in 0 ..< 1000 {
        actor.wait_idle(sched)
        stats = actor.call(sup, actor.Supervisor_Msg(actor.Get_Supervisor_Stats{}), actor.Supervisor_Stat
        if stats.total_restarts >= expected {
            break
        }
        time.sleep(100 * time.Microsecond)
    }
    return stats
}
run_demo :: proc() {
    sched := actor.scoped_scheduler(num_workers = 4)
    reg: actor.Registry(u64)
    actor.registry_init(&reg)
    defer actor.registry_destroy(&reg)
    dyn_sup: actor.Dynamic_Supervisor
    actor.dynamic_supervisor_init(
        ds           = &dyn_sup,
        idle_timeout = 50 * time.Millisecond,
        registry     = &reg,
        s            = sched,
        factory      = proc(s: ^actor.Scheduler, org_id: u64) -> actor.Tenant_Subtree {
            slot := slot_for_org(org_id)
            slot.committed_moment = 1000 // Seed initial durable moment
            // `.Rest_For_One` supervisor for `Org_<id>`:
            // Child 0 (`Xitdb_Owner`) -> Child 1 (`Org_Router`) -> Child 2 (`Bot_Worker`)
            org_sup := actor.spawn_supervisor(
                strategy      = .Rest_For_One,
                max_restarts  = 5,
                within_window = 10 * time.Second,
                s             = s,
            )
            db_init  := Xitdb_Owner_State{org_id = org_id, slot = slot, handles = 1}
            db_actor := actor.spawn(s, db_init, xitdb_owner_receive, id = u32(org_id * 10 + 1))
            bot_init  := Bot_Worker_State{org_id = org_id}
            bot_actor := actor.spawn(s, bot_init, bot_worker_receive, id = u32(org_id * 10 + 3))
            router_init := Org_Router_State{
                org_id     = org_id,
                slot       = slot,
                db_actor   = db_actor,
                bot_actor  = bot_actor,
                live_value = slot.committed_moment,
            }
            router_actor := actor.spawn(s, router_init, org_router_receive, id = u32(org_id * 10 + 2))
            // Register in dependency order: [0] Xitdb_Owner, [1] Org_Router, [2] Bot_Worker
            actor.supervise(org_sup, db_actor, db_init, .Permanent, use_arena = true, keep_mailbox = fa
            actor.supervise(org_sup, router_actor, router_init, .Permanent, org_router_reinit, use_aren
            actor.supervise(org_sup, bot_actor, bot_init, .Permanent, use_arena = true, keep_mailbox =
            actor.wait_idle(s)
            fmt.printfln("[Dynamic_Supervisor] Spawned `.Rest_For_One` tree for Org_%d (initial moment
                org_id, slot.committed_moment)
            subtree := actor.Tenant_Subtree{
                sup      = org_sup,
                router   = &router_actor.base,
                msg_type = typeid_of(Router_Msg),
                user_ctx = slot,
            }
            append(&subtree.children, &db_actor.base)
            append(&subtree.children, &router_actor.base)
            append(&subtree.children, &bot_actor.base)
            return subtree
        },
        on_evict = proc(org_id: u64, subtree: ^actor.Tenant_Subtree) {
            slot := cast(^Xitdb_Slot)subtree.user_ctx
            if slot != nil {
                slot.flushed_to_cloud = true
                fmt.printfln("[Dynamic_Supervisor] Evicting idle Org_%d -> flushed xitdb moment %d t
                    org_id, slot.committed_moment)
            }
        },
    )
    defer actor.dynamic_supervisor_destroy(&dyn_sup)
    // 1. Send normal traffic to Org_101 (+250) and Org_102 (+500)
    fmt.println("\n--- Step 1: Normal Multi-Tenant Turn Processing ---")
    actor.send_to_tenant(&dyn_sup, 101, Router_Msg(Route_Turn{delta = 250, scratch_bytes = 4096}))
    actor.send_to_tenant(&dyn_sup, 102, Router_Msg(Route_Turn{delta = 500, scratch_bytes = 4096}))
    actor.wait_idle(sched)
    st_101 := actor.call_tenant(&dyn_sup, 101, Router_Msg(Route_Query_Status{}), Router_Status)
    st_102 := actor.call_tenant(&dyn_sup, 102, Router_Msg(Route_Query_Status{}), Router_Status)
    fmt.printfln("  Org_101 live_value = %d | Org_102 live_value = %d", st_101.live_value, st_102.live_value
    // 2. Crash Org_101's Router with an out-of-bounds slice panic (`headers[99]`):
    //    - `Org_Router` (Child 1) and `Bot_Worker` (Child 2) restart under `.Rest_For_One`
    //    - `Xitdb_Owner` (Child 0) is untouched (`db_gen == 0`)
    //    - `Org_Router` wipes its half-mutated `-999_999` state + 16 KiB scratch buffer
    //      and rehydrates `live_value = 1250` from `Xitdb_Slot`!
    fmt.println("\n--- Step 2: Trapping Out-of-Bounds Slice Panic in Org_101 Router ---")
    org_101_sup := dyn_sup.tenants[101].subtree.sup
    actor.send_to_tenant(&dyn_sup, 101, Router_Msg(Route_Malformed_Payload{oob_index = 99}))
    sup_stats := wait_for_restarts(sched, org_101_sup, expected = 2)
    st_101 = actor.call_tenant(&dyn_sup, 101, Router_Msg(Route_Query_Status{}), Router_Status)
    st_102 = actor.call_tenant(&dyn_sup, 102, Router_Msg(Route_Query_Status{}), Router_Status)
    fmt.printfln("  Org_101 Supervisor restarts = %d (reason = %v at %s:%d)",
        sup_stats.total_restarts, sup_stats.last_fault.reason,
        sup_stats.last_fault.loc.procedure, sup_stats.last_fault.loc.line)
    fmt.printfln("  Org_101 rehydrated live_value = %d (db_gen=%d, bot_gen=%d) | Org_102 live_value = %d",
        st_101.live_value, st_101.db_gen, st_101.bot_gen, st_102.live_value)
    // 3. Crash Org_101's `Xitdb_Owner` (Child 0) to demonstrate `.Rest_For_One` full cascade:
    //    Crashing Child 0 restarts Child 0 (`Xitdb_Owner`), Child 1 (`Org_Router`), and Child 2 (`Bot_Worke
    fmt.println("\n--- Step 3: `.Rest_For_One` Cascade When Org_101 Xitdb_Owner Crashes ---")
    actor.send_to_tenant(&dyn_sup, 101, Router_Msg(Route_Crash_Db{}))
    sup_stats = wait_for_restarts(sched, org_101_sup, expected = 5) // 2 prior + 3 children = 5
    // Send +50 more to Org_101 using the exact same tenant route after the cascade restart:
    actor.send_to_tenant(&dyn_sup, 101, Router_Msg(Route_Turn{delta = 50, scratch_bytes = 1024}))
    actor.wait_idle(sched)
    st_101 = actor.call_tenant(&dyn_sup, 101, Router_Msg(Route_Query_Status{}), Router_Status)
    fmt.printfln("  Org_101 post-cascade live_value = %d (db_gen=%d, bot_gen=%d, total_restarts=%d)",
        st_101.live_value, st_101.db_gen, st_101.bot_gen, sup_stats.total_restarts)
    // 4. Sweep idle tenants after 100ms (keeping Org_102 active, evicting idle Org_101):
    fmt.println("\n--- Step 4: Idle Tenant Eviction & Tigris Flush ---")
    future := time.tick_add(time.tick_now(), 100 * time.Millisecond)
    dyn_sup.tenants[102].last_active = future
    evicted := actor.sweep_idle_tenants(&dyn_sup, now_override = future)
    _, ok_101 := actor.registry_lookup(&reg, 101, Router_Msg)
    _, ok_102 := actor.registry_lookup(&reg, 102, Router_Msg)
    fmt.printfln("  Evicted %d idle tenant(s) | Org_101 in registry: %v (flushed=%v) | Org_102 in registry:
        evicted, ok_101, g_org_slots[0].flushed_to_cloud, ok_102)
}
main :: proc() {
    track: mem.Tracking_Allocator
    mem.tracking_allocator_init(&track, context.allocator)
    defer mem.tracking_allocator_destroy(&track)
    context.allocator = mem.tracking_allocator(&track)
    run_demo()
    free_all(context.temp_allocator)
    fmt.printfln("\nMemory leak check: %d leaked allocations, %d bad frees.",
        len(track.allocation_map), len(track.bad_free_array))
    assert(len(track.allocation_map) == 0)
    assert(len(track.bad_free_array) == 0)
}
