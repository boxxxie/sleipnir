package sleipnir_tests
import "core:mem"
import "core:testing"
import "core:time"
import "../actor"
// =============================================================================
// Layer 1: Executable Specifications
// =============================================================================
@(test)
test_20_one_for_one_traps_bounds_panic_wipes_arena_and_preserves_pool :: proc(t: ^testing.T) {
    with_leak_check(t, proc(t: ^testing.T) {
        w := supervision_world(t, actor.scoped_scheduler(num_workers = 2))
        given_supervisor(&w, strategy = .One_For_One, max_restarts = 5)
        given_supervised_tenant_worker(&w, committed_txs = 10)
        // Send a batch where the 2nd message triggers a slice out-of-bounds panic (`items[99]`),
        // followed by a valid deposit after the supervisor restarts the worker in-place:
        upon_processing_tenant_batch(&w,
            Tenant_Add{delta = 5, scratch_bytes = 4096},
            Tenant_Force_Bounds_Panic{bad_index = 99},
        )
        then_supervisor_restarts_are(&w, expected_restarts = 1, expected_reason = .Panic_Trapped)
        // Verify the same `PID` / `^Actor` handle still works after restart and rehydrated
        // from `committed_txs = 10`, then applied `+ 25 = 35`:
        upon_sending_tenant_msg(&w, Tenant_Add{delta = 25, scratch_bytes = 1024})
        then_tenant_balance_is(&w, expected = 35)
    })
}
@(test)
test_21_rest_for_one_restarts_downstream_dependents_in_order :: proc(t: ^testing.T) {
    with_leak_check(t, proc(t: ^testing.T) {
        w := supervision_world(t, actor.scoped_scheduler(num_workers = 2))
        given_supervisor(&w, strategy = .Rest_For_One, max_restarts = 5)
        given_supervised_pipeline(&w) // Child 0: `db_owner`, Child 1: `router`, Child 2: `worker`
        // Crashing Child 1 (`router`) under `.Rest_For_One` must leave Child 0 (`db_owner`)
        // untouched (generation 0) while restarting Child 1 (`router`) and Child 2 (`worker`):
        upon_crashing_pipeline_stage(&w, stage = 1)
        then_supervisor_restarts_are(&w, expected_restarts = 2, expected_reason = .Panic_Trapped)
        then_stage_generations_are(&w, gen_0 = 0, gen_1 = 1, gen_2 = 1)
    })
}
// =============================================================================
// Layer 2 & 3: Supervision Scenario DSL & Protocol Driver
// =============================================================================
@(private = "file")
Tenant_Add :: struct {
    delta:         i64,
    scratch_bytes: int,
}
@(private = "file")
Tenant_Force_Bounds_Panic :: struct {
    bad_index: int,
}
@(private = "file")
Tenant_Get_Total :: struct {}
@(private = "file")
Tenant_Msg :: union {
    Tenant_Add,
    Tenant_Force_Bounds_Panic,
    Tenant_Get_Total,
}
@(private = "file")
Tenant_State :: struct {
    total:        i64,
    last_scratch: []byte,
}
@(private = "file")
Supervision_World :: struct {
    t:      ^testing.T,
    sched:  ^actor.Scheduler,
    sup:    ^actor.Actor(actor.Supervisor_State, actor.Supervisor_Msg),
    tenant: ^actor.Actor(Tenant_State, Tenant_Msg),
    stages: [3]^actor.Actor(Tenant_State, Tenant_Msg),
}
@(private = "file")
supervision_world :: proc(t: ^testing.T, sched: ^actor.Scheduler) -> Supervision_World {
    return Supervision_World{t = t, sched = sched}
}
@(private = "file")
tenant_behavior :: proc(self: ^Tenant_State, msg: Tenant_Msg) {
    switch m in msg {
    case Tenant_Add:
        if m.scratch_bytes > 0 {
            // Allocated from the actor's private `vmem.Arena` (`context.allocator`)
            self.last_scratch = make([]byte, m.scratch_bytes)
        }
        self.total += m.delta
    case Tenant_Force_Bounds_Panic:
        // Allocate inside the actor's arena right before crashing to prove O(1) arena wipe
        self.last_scratch = make([]byte, 8192)
        arr := [2]int{1, 2}
        _ = arr[m.bad_index] // Triggers `context.assertion_failure_proc` -> `longjmp`!
    case Tenant_Get_Total:
        actor.reply(self.total)
    }
}
@(private = "file")
given_supervisor :: proc(
    w: ^Supervision_World,
    strategy: actor.Restart_Strategy,
    max_restarts: int,
) {
    w.sup = actor.spawn_supervisor(
        strategy      = strategy,
        max_restarts  = max_restarts,
        within_window = 10 * time.Second,
        s             = w.sched,
    )
}
@(private = "file")
given_supervised_tenant_worker :: proc(w: ^Supervision_World, committed_txs: i64) {
    init_state := Tenant_State{total = committed_txs}
    w.tenant = actor.spawn(w.sched, init_state, tenant_behavior)
    actor.supervise(w.sup, w.tenant, init_state, .Permanent, use_arena = true, keep_mailbox = false)
    actor.wait_idle(w.sched)
}
@(private = "file")
given_supervised_pipeline :: proc(w: ^Supervision_World) {
    for i := 0; i < 3; i += 1 {
        init_state := Tenant_State{total = i64(i + 1) * 100}
        w.stages[i] = actor.spawn(w.sched, init_state, tenant_behavior, id = u32(i))
        actor.supervise(w.sup, w.stages[i], init_state, .Permanent, use_arena = true, keep_mailbox = fals
    }
    actor.wait_idle(w.sched)
}
@(private = "file")
upon_processing_tenant_batch :: proc(w: ^Supervision_World, msgs: ..Tenant_Msg) {
    actor.send(w.tenant, msgs)
}
@(private = "file")
upon_sending_tenant_msg :: proc(w: ^Supervision_World, msg: Tenant_Msg) {
    actor.send(w.tenant, msg)
}
@(private = "file")
upon_crashing_pipeline_stage :: proc(w: ^Supervision_World, stage: int) {
    actor.send(w.stages[stage], Tenant_Msg(Tenant_Force_Bounds_Panic{bad_index = 42}))
}
@(private = "file")
then_supervisor_restarts_are :: proc(
    w: ^Supervision_World,
    expected_restarts: int,
    expected_reason: actor.Exit_Reason,
    loc := #caller_location,
) {
    // Poll supervisor stats via synchronous `actor.call` until the async `Child_Down` completes
    stats: actor.Supervisor_Stats
    for _ in 0 ..< 1000 {
        actor.wait_idle(w.sched)
        stats = actor.call(w.sup, actor.Supervisor_Msg(actor.Get_Supervisor_Stats{}), actor.Supervisor_St
        if stats.total_restarts >= expected_restarts {
            break
        }
        time.sleep(100 * time.Microsecond)
    }
    testing.expect_value(w.t, stats.total_restarts, expected_restarts, loc = loc)
    testing.expect_value(w.t, stats.last_fault.reason, expected_reason, loc = loc)
}
@(private = "file")
then_tenant_balance_is :: proc(
    w: ^Supervision_World,
    expected: i64,
    loc := #caller_location,
) {
    actual := actor.call(w.tenant, Tenant_Msg(Tenant_Get_Total{}), i64)
    testing.expect_value(w.t, actual, expected, loc = loc)
}
@(private = "file")
then_stage_generations_are :: proc(
    w: ^Supervision_World,
    gen_0, gen_1, gen_2: u32,
    loc := #caller_location,
) {
    testing.expect_value(w.t, w.stages[0].base.generation, gen_0, loc = loc)
    testing.expect_value(w.t, w.stages[1].base.generation, gen_1, loc = loc)
    testing.expect_value(w.t, w.stages[2].base.generation, gen_2, loc = loc)
}
