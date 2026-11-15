// In actor/actor.odin:
import vmem "core:mem/virtual"
import "base:runtime"
ACTOR_STATUS_RUNNABLE :: u32(0)
ACTOR_STATUS_FAULTED  :: u32(1)
ACTOR_STATUS_STOPPED  :: u32(2)
CALL_READY_WAITING :: u32(0)
CALL_READY_OK      :: u32(1)
CALL_READY_FAILED  :: u32(2)
Exit_Reason :: enum u8 {
    Normal,
    Shutdown,
    Panic_Trapped,
    Call_Target_Crashed,
    Invalid_State,
}
Fault_Info :: struct {
    actor_id:   u32,
    generation: u32,
    reason:     Exit_Reason,
    prefix:     string,
    message:    string,
    loc:        runtime.Source_Code_Location,
}
Actor_Base :: struct #align(64) {
    // --- Cache Line 0 (64 bytes) ---
    next:         ^Actor_Base, // Offset 0: Intrusive link for `RunQ_Shard(Actor_Base)`
    scheduled:    u32,         // Offset 8: Atomic flag: 0 = idle, 1 = queued/running
    id:           u32,         // Offset 12: Actor ID for routing / supervision
    user_ptr:     rawptr,      // Offset 16: Optional user context pointer
    step_proc:    proc(base: ^Actor_Base, ctx: ^Context, quota: int) -> (processed: int, has_more: bool),
    destroy_proc: proc(base: ^Actor_Base),
    reg_sched:    ^Scheduler,
    status:       u32,         // Offset 48: Atomic `ACTOR_STATUS_*`
    generation:   u32,         // Offset 52: Incremented on restart/stop
    // --- Cache Line 1 (Supervision & Per-Actor Arena) ---
    reset_proc:   proc(base: ^Actor_Base), // Rehydrates `self.state` on supervisor restart
    supervisor:   rawptr,                  // Optional `^Actor_Ref(Supervisor_Msg)` monitor
    has_arena:    bool,
    keep_mailbox: bool,                    // true = preserve queued msgs on restart; false = drain to pool
    arena:        vmem.Arena,              // Per-actor living state arena (O(1) wipe on crash)
}
