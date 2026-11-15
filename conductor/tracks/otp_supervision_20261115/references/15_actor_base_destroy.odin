// Add to actor/supervision.odin:
// Safely destroy a type-erased `^Actor_Base` at runtime, removing it from
// `s.owned_actors` and invoking its `destroy_proc` (which frees its `vmem.Arena`,
// supervision descriptor, and actor allocation).
actor_base_destroy :: proc(base: ^Actor_Base) {
    if base == nil {
        return
    }
    intrinsics.atomic_store_explicit(&base.status, ACTOR_STATUS_STOPPED, .Seq_Cst)
    s := base.reg_sched
    if s != nil {
        if s.started && intrinsics.atomic_load_explicit(&s.running, .Acquire) {
            for intrinsics.atomic_load_explicit(&base.scheduled, .Acquire) != 0 {
                intrinsics.cpu_relax()
            }
        }
        sync.mutex_lock(&s.registry_mu)
        for i := 0; i < len(s.owned_actors); i += 1 {
            if s.owned_actors[i] == base {
                unordered_remove(&s.owned_actors, i)
                break
            }
        }
        sync.mutex_unlock(&s.registry_mu)
        base.reg_sched = nil
    }
    if base.destroy_proc != nil {
        base.destroy_proc(base)
    }
}
