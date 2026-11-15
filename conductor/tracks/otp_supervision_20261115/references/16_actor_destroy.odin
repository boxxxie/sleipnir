// In actor/actor.odin:
actor_destroy :: proc(a: ^Actor($State, $Msg)) {
    if a == nil do return
    if a.base.destroy_proc != nil {
        actor_base_destroy(&a.base)
        return
    }
    s := a.base.reg_sched
    if s != nil {
        if s.started && intrinsics.atomic_load_explicit(&s.running, .Acquire) {
            for intrinsics.atomic_load_explicit(&a.base.scheduled, .Acquire) != 0 {
                intrinsics.cpu_relax()
            }
        }
        sync.mutex_lock(&s.registry_mu)
        for i := 0; i < len(s.owned_actors); i += 1 {
            if s.owned_actors[i] == &a.base {
                unordered_remove(&s.owned_actors, i)
                break
            }
        }
        sync.mutex_unlock(&s.registry_mu)
        a.base.reg_sched = nil
    }
    free(a, a.allocator)
}
