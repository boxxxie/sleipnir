    pending := make([dynamic]Behavior_Spec)
    defer delete(pending)
    pending_reinits := make([dynamic]Reinit_Spec)
    defer delete(pending_reinits)
    // ... pass `&pending_reinits` into `parse_source_into_spec`
    // and `pending_reinits[:]` into `finalize_module_spec(&mod, pending[:], pending_reinits[:])`
