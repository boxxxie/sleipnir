// =============================================================================
// 4. Thread-Safe Named Registry (`Registry($Key)`)
// =============================================================================
Registry_Entry :: struct {
    ref:      ^Actor_Base,
    msg_type: typeid,
}
Registry :: struct($Key: typeid) {
    mu:        sync.RW_Mutex,
    entries:   map[Key]Registry_Entry,
    allocator: mem.Allocator,
}
registry_init :: proc(r: ^Registry($Key), allocator := context.allocator) {
    r.allocator = allocator
    r.entries   = make(map[Key]Registry_Entry, 64, allocator)
}
registry_destroy :: proc(r: ^Registry($Key)) {
    sync.rw_mutex_lock(&r.mu)
    delete(r.entries)
    sync.rw_mutex_unlock(&r.mu)
}
// Register a typed `^Actor($State, $Msg)` under `key`.
registry_register_actor :: proc(r: ^Registry($Key), key: Key, a: ^Actor($State, $Msg)) {
    sync.rw_mutex_lock(&r.mu)
    r.entries[key] = Registry_Entry{
        ref      = &a.base,
        msg_type = typeid_of(Msg),
    }
    sync.rw_mutex_unlock(&r.mu)
}
// Register a message-typed `PID($Msg)` under `key`.
registry_register_pid :: proc(r: ^Registry($Key), key: Key, p: PID($Msg)) {
    sync.rw_mutex_lock(&r.mu)
    r.entries[key] = Registry_Entry{
        ref      = &p.ref.base,
        msg_type = typeid_of(Msg),
    }
    sync.rw_mutex_unlock(&r.mu)
}
registry_register :: proc {
    registry_register_actor,
    registry_register_pid,
}
registry_unregister :: proc(r: ^Registry($Key), key: Key) {
    sync.rw_mutex_lock(&r.mu)
    delete_key(&r.entries, key)
    sync.rw_mutex_unlock(&r.mu)
}
// Concurrent read-locked lookup returning a strongly-typed `PID($Msg)`.
// Returns `(_, false)` if `key` is not registered, if `$Msg` mismatches, or if the actor is stopped.
@(require_results)
registry_lookup :: proc(r: ^Registry($Key), key: Key, $Msg: typeid) -> (pid_out: PID(Msg), ok: bool) {
    sync.rw_mutex_shared_lock(&r.mu)
    entry, found := r.entries[key]
    sync.rw_mutex_shared_unlock(&r.mu)
    if !found || entry.ref == nil || entry.msg_type != typeid_of(Msg) {
        return {}, false
    }
    if intrinsics.atomic_load_explicit(&entry.ref.status, .Acquire) == ACTOR_STATUS_STOPPED {
        return {}, false
    }
    return PID(Msg){ref = cast(^Actor_Ref(Msg))entry.ref}, true
}
