parse_source_into_spec :: proc(
    src: string,
    filename: string,
    mod: ^Module_Spec,
    pending_behaviors: ^[dynamic]Behavior_Spec,
    pending_reinits: ^[dynamic]Reinit_Spec = nil,
) -> bool {
    // ... existing AST setup ...
        case ^ast.Value_Decl:
            if d.is_mutable || len(d.names) == 0 || len(d.values) == 0 {
                continue
            }
            ident, ok := d.names[0].derived.(^ast.Ident)
            if !ok do continue
            if _, is_actor := get_attribute(d, "actor"); is_actor {
                if _, is_struct := d.values[0].derived.(^ast.Struct_Type); is_struct {
                    sup_val, has_sup := get_attribute(d, "supervise")
                    policy := sup_val if len(sup_val) > 0 else "Permanent"
                    already_exists := false
                    for &existing in mod.actors {
                        if existing.name == ident.name {
                            if has_sup {
                                existing.has_supervise    = true
                                existing.supervise_policy = policy
                            }
                            already_exists = true
                            break
                        }
                    }
                    if !already_exists {
                        append(&mod.actors, Actor_Spec{
                            name             = ident.name,
                            behaviors        = make([dynamic]Behavior_Spec),
                            has_supervise    = has_sup,
                            supervise_policy = policy,
                        })
                    }
                }
                continue
            }
            // Check for `@(reinit)` before treating the procedure as a message behavior:
            if _, is_reinit := get_attribute(d, "reinit"); is_reinit {
                proc_lit, is_proc := d.values[0].derived.(^ast.Proc_Lit)
                if is_proc && proc_lit.type != nil && proc_lit.type.params != nil {
                    fields := proc_lit.type.params.list
                    if len(fields) >= 1 {
                        if actor_type, ok0 := extract_pointer_ident(fields[0].type); ok0 {
                            param_count := 0
                            for f in fields {
                                param_count += max(1, len(f.names))
                            }
                            if pending_reinits != nil {
                                append(pending_reinits, Reinit_Spec{
                                    proc_name   = ident.name,
                                    actor_type  = actor_type,
                                    takes_alloc = param_count >= 2,
                                })
                            }
                        }
                    }
                }
                continue
            }
