// Current loop in actor/actor.odin:
collector: Batch_Return_Collector(Msg)
has_pools := len(self.pools) > 0
processed := 0
for processed < quota {
    env := mpsc_pop_detached(&self.mailbox)
    if env == nil do break
    ...
    self.receive(ctx, self, &env.msg) // <-- What if message #10 of 64 panics here?
    processed += 1
    if !ctx.forwarded {
        collector_add(&collector, env)
    }
}
if processed > 0 && has_pools {
    collector_flush(&collector, self.pools, -1)
}
