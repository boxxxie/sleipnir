// 1. Add `Reinit_Spec` and extend `Actor_Spec` (actor/codegen/codegen.odin:41-55):
Reinit_Spec :: struct {
    proc_name:   string,
    actor_type:  string,
    takes_alloc: bool,
}
Actor_Spec :: struct {
    name:             string,
    behaviors:        [dynamic]Behavior_Spec,
    has_supervise:    bool,
    supervise_policy: string, // "Permanent" (default), "Transient", or "Temporary"
    reinit_proc:      string,
    reinit_has_alloc: bool,
}
