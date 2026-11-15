scheduler_schedule_actor :: #force_inline proc(s: ^Scheduler, a: ^Actor_Base, shard_hint: int = 0) {
    if intrinsics.atomic_load_explicit(&a.status, .Acquire) != ACTOR_STATUS_RUNNABLE {
        return
    }
    if intrinsics.atomic_load_explicit(&a.scheduled, .Seq_Cst) == 0 {
        if intrinsics.atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0 {
            // Re-verify status after acquiring `scheduled = 1`
            if intrinsics.atomic_load_explicit(&a.status, .Acquire) != ACTOR_STATUS_RUNNABLE {
                intrinsics.atomic_store_explicit(&a.scheduled, 0, .Seq_Cst)
                return
            }
            intrinsics.atomic_add_explicit(&s.active_actors, 1, .Acq_Rel)
            idx := u32(shard_hint) % u32(s.num_shards)
            shard_push(&s.shards[idx], a)
            if s.enable_parking {
                if intrinsics.atomic_load_explicit(&s.parked_workers, .Acquire) > 0 {
                    intrinsics.atomic_add_explicit((^u32)(&s.wake_futex), 1, .Release)
                    sync.futex_signal(&s.wake_futex)
                }
            }
        }
    }
}
