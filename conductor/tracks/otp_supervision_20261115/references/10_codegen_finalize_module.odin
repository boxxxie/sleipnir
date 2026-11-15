finalize_module_spec :: proc(
    mod: ^Module_Spec,
    pending_behaviors: []Behavior_Spec,
    pending_reinits: []Reinit_Spec = nil,
) {
    for b in pending_behaviors {
        matched := false
        for &a in mod.actors {
            if a.name == b.actor_type {
                append(&a.behaviors, b)
                matched = true
                break
            }
        }
        if !matched {
            delete(b.params)
        }
    }
    for r in pending_reinits {
        for &a in mod.actors {
            if a.name == r.actor_type {
                a.reinit_proc      = r.proc_name
                a.reinit_has_alloc = r.takes_alloc
                a.has_supervise    = true
                if len(a.supervise_policy) == 0 {
                    a.supervise_policy = "Permanent"
                }
                break
            }
        }
    }
}
