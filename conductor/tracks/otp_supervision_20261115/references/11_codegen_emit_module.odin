        // 5. Static VTable instance, optional reinit hook, & `spawn_<actor>` constructors
        fmt.sbprintf(&sb, "@(private)\n%s_vtable := %s_VTable{{\n", snake_actor, a.name)
        for b in a.behaviors {
            if len(b.return_type) > 0 {
                fmt.sbprintf(&sb, "\t%s = call_%s_%s_actor,\n", b.short_name, snake_actor, b.short_n
            } else {
                fmt.sbprintf(&sb, "\t%s = send_%s_%s_actor,\n", b.short_name, snake_actor, b.short_n
            }
        }
        fmt.sbprintln(&sb, "}\n")
        reinit_cb := "nil"
        if len(a.reinit_proc) > 0 {
            reinit_cb = fmt.tprintf("%s_reinit_hook", snake_actor)
            fmt.sbprintf(&sb, "@(private)\n%s :: proc(self: ^%s, alloc: actor.Allocator) {{\n", reinit_
            if a.reinit_has_alloc {
                fmt.sbprintf(&sb, "\t%s(self, alloc)\n", a.reinit_proc)
            } else {
                fmt.sbprintf(&sb, "\t_ = alloc\n\t%s(self)\n", a.reinit_proc)
            }
            fmt.sbprintln(&sb, "}\n")
        }
        fmt.sbprintf(&sb, "spawn_%s_auto :: proc(\n", snake_actor)
        fmt.sbprintf(&sb, "\tinitial_state: %s = {{}},\n", a.name)
        fmt.sbprintln(&sb, "\tid: u32 = 0,")
        fmt.sbprintln(&sb, "\tallocator := context.allocator,")
        fmt.sbprintf(&sb, ") -> %s_Actor {{\n", a.name)
        fmt.sbprintf(&sb, "\ta, _ := actor.spawn(initial_state, %s_dispatch, id, allocator)\n", snake_act
        if a.has_supervise {
            fmt.sbprintf(&sb, "\t_ = actor.enable_actor_supervision(a, initial_state, %s)\n", reinit_cb
        }
        fmt.sbprintf(&sb, "\treturn %s_Actor{{raw = a, vtable = &%s_vtable}}\n", a.name, snake_actor)
        fmt.sbprintln(&sb, "}\n")
        fmt.sbprintf(&sb, "spawn_%s_sched :: proc(\n", snake_actor)
        fmt.sbprintln(&sb, "\ts: ^actor.Scheduler,")
        fmt.sbprintf(&sb, "\tinitial_state: %s = {{}},\n", a.name)
        fmt.sbprintln(&sb, "\tid: u32 = 0,")
        fmt.sbprintln(&sb, "\tallocator := context.allocator,")
        fmt.sbprintf(&sb, ") -> %s_Actor {{\n", a.name)
        fmt.sbprintf(&sb, "\ta, _ := actor.spawn(s, initial_state, %s_dispatch, id, allocator)\n", snake_
        if a.has_supervise {
            fmt.sbprintf(&sb, "\t_ = actor.enable_actor_supervision(a, initial_state, %s)\n", reinit_cb
        }
        fmt.sbprintf(&sb, "\treturn %s_Actor{{raw = a, vtable = &%s_vtable}}\n", a.name, snake_actor)
        fmt.sbprintln(&sb, "}\n")
        if a.has_supervise {
            policy := a.supervise_policy if len(a.supervise_policy) > 0 else "Permanent"
            fmt.sbprintf(&sb, "spawn_%s_sup :: proc(\n", snake_actor)
            fmt.sbprintln(&sb, "\tsup: ^actor.Actor(actor.Supervisor_State, actor.Supervisor_Msg),")
            fmt.sbprintf(&sb, "\tinitial_state: %s = {{}},\n", a.name)
            fmt.sbprintf(&sb, "\tpolicy: actor.Restart_Policy = .%s,\n", policy)
            fmt.sbprintln(&sb, "\tid: u32 = 0,")
            fmt.sbprintln(&sb, "\tallocator := context.allocator,")
            fmt.sbprintf(&sb, ") -> %s_Actor {{\n", a.name)
            fmt.sbprintf(&sb, "\ta, _ := actor.spawn(sup.sched, initial_state, %s_dispatch, id, allocat
            fmt.sbprintf(&sb, "\tactor.supervise(sup, a, initial_state, policy, %s)\n", reinit_cb)
            fmt.sbprintf(&sb, "\treturn %s_Actor{{raw = a, vtable = &%s_vtable}}\n", a.name, snake_acto
            fmt.sbprintln(&sb, "}\n")
        }
        fmt.sbprintf(&sb, "spawn_%s :: proc {{\n", snake_actor)
        fmt.sbprintf(&sb, "\tspawn_%s_auto,\n", snake_actor)
        fmt.sbprintf(&sb, "\tspawn_%s_sched,\n", snake_actor)
        if a.has_supervise {
            fmt.sbprintf(&sb, "\tspawn_%s_sup,\n", snake_actor)
        }
        fmt.sbprintln(&sb, "}\n")
