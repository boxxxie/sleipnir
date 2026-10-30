# Sleipnir (`package actor`)

High-throughput, zero-GC, Pony-style actor runtime for [Odin](https://odin-lang.org/).

Sleipnir combines Vyukov Intrusive MPSC mailboxes, a sharded work-stealing `M:N` thread scheduler, lock-free `Envelope_Pool` slab recycling, and a wait-free `Sharded_SPSC_Mailbox`—all integrated with Odin's `context.allocator` and explicit `destroy` lifecycle.

- **Zero Garbage Collector:** Deterministic memory management via `actor.Envelope_Pool(Msg)` or custom `mem.Allocator`.
- **Heterogeneous Actors:** Spawn arbitrarily many actor types `(State, Msg)` onto a single shared `actor.Scheduler`.
- **Overloaded `actor API`:** Ergonomic proc groups (`actor.init`, `actor.destroy`, `actor.send`, `actor.forward`) work from both external threads and inside actor handlers.
- **Performance:** **31–78M msgs/sec** on 4-producer MPSC fan-in, **827M msgs/sec** (`1.2 ns/msg`) on wait-free sharded SPSC, and **16.65M hops/sec** across 1,024 autonomous actors on 8 cores—matching or outperforming the official Pony C runtime (`libponyrt` 0.77.2) and raw C11 (`stdatomic.h`). See the full [**Benchmark Report & Cross-Runtime Comparison (`benchmarks/README.md`)**](benchmarks/README.md).

---

## Quick Start

Import `./actor` into your Odin project and run tests or examples:

```bash
odin test actor
odin run examples/01_ping_pong.odin -file
odin run examples/02_heterogeneous_pipeline.odin -file
odin run examples/03_sharded_spsc_mailbox.odin -file
```

---

## Examples

### 1. Basic Actor & Pooled Message Passing

Create a [`Scheduler`](actor/scheduler.odin), an [`Envelope_Pool`](actor/pool.odin) for zero-allocation message recycling, and spawn an actor with [`actor.spawn`](actor/scheduler.odin):

```odin
package main

import "core:fmt"
import "core:sync"
import "actor"

Counter :: struct {
    total: int,
    done:  ^sync.Sema,
}

Add_Msg :: struct {
    amount: int,
    final:  bool,
}

handle_add :: proc(ctx: ^actor.Context, state: ^Counter, msg: ^Add_Msg) {
    state.total += msg.amount
    if msg.final {
        fmt.printf("Total: %d\n", state.total)
        sync.sema_post(state.done)
    }
}

main :: proc() {
    sched: actor.Scheduler
    actor.init(&sched, num_workers = 2)
    defer actor.destroy(&sched)

    pool: actor.Envelope_Pool(Add_Msg)
    actor.init(&pool, num_threads = 1, slots_per_shard = 1024)
    defer actor.destroy(&pool)

    actor.start(&sched)

    done: sync.Sema
    counter := actor.spawn(&sched, Counter{done = &done}, handle_add)

    // Send 1,000 pooled messages from the main thread (shard 0)
    for i in 1 ..= 1000 {
        actor.send(&sched, &pool, counter, Add_Msg{
            amount = i,
            final  = (i == 1000),
        })
    }

    sync.sema_wait(&done) // Prints: Total: 500500
}
```

---

### 2. Batched Message Splicing (`Chain=K`)

When a producer emits multiple messages to the same actor, passing a slice `[]Msg` to [`actor.send`](actor/actor.odin) links all $K$ envelopes locally in thread-local storage and splices the entire chain into the target actor's mailbox with **a single atomic `XCHG`** (reducing atomic contention by $K\times$):

```odin
batch := [4]Add_Msg{
    {amount = 10},
    {amount = 20},
    {amount = 30},
    {amount = 40, final = true},
}

// Links all 4 envelopes locally and performs 1 atomic XCHG on `counter`'s mailbox
actor.send(&sched, &pool, counter, batch[:], producer_shard = 0)
```

---

### 3. Zero-Copy `iso` Message Forwarding (`actor.forward`)

In Pony-style reference capabilities, an isolated (`iso`) message envelope can be mutated in place and forwarded to another actor without allocating or freeing a new envelope. Calling [`actor.forward`](actor/actor.odin) inside a handler transfers ownership of the current envelope directly to the next actor:

```odin
package main

import "core:fmt"
import "core:sync"
import "actor"

Ball :: struct {
    hops_left: int,
}

Player :: struct {
    partner: actor.PID(Ball),
    done:    ^sync.Sema,
}

on_ball :: proc(ctx: ^actor.Context, self: ^Player, ball: ^Ball) {
    if ball.hops_left <= 0 {
        sync.sema_post(self.done)
        return
    }
    ball.hops_left -= 1
    // Mutate `ball` in place and forward the existing envelope with zero allocation!
    actor.forward(ctx, self.partner, ball)
}

main :: proc() {
    sched: actor.Scheduler
    actor.init(&sched, num_workers = 2)
    defer actor.destroy(&sched)

    done: sync.Sema
    ping := actor.spawn(&sched, Player{done = &done}, on_ball)
    pong := actor.spawn(&sched, Player{partner = ping, done = &done}, on_ball)
    ping.raw.user_state = rawptr(&Player{partner = pong, done = &done}) // or send init msg

    actor.start(&sched)

    // Heap-allocated one-off message (freed automatically when no longer forwarded)
    actor.send(&sched, ping, Ball{hops_left = 100_000})
    sync.sema_wait(&done)
}
```

> See [`examples/01_ping_pong.odin`](examples/01_ping_pong.odin) for the complete runnable ping-pong example.

---

### 4. Heterogeneous Actors on a Shared Scheduler

Multiple actor types with distinct `(State, Msg)` pairs coexist on the same [`actor.Scheduler`](actor/scheduler.odin). Inside an actor handler, pass `ctx` (`^actor.Context`) to `actor.send` to automatically route via the current worker thread's shard without atomic contention:

```odin
// Worker actor state & message
Work_Req :: struct {
    value:    u64,
    reply_to: actor.PID(Work_Result),
}
Worker :: struct {
    result_pool: ^actor.Envelope_Pool(Work_Result),
}

// Aggregator actor state & message
Work_Result :: struct {
    hashed: u64,
}
Aggregator :: struct {
    received: int,
}

worker_receive :: proc(ctx: ^actor.Context, state: ^Worker, req: ^Work_Req) {
    res := Work_Result{hashed = req.value ~ 0x9e3779b97f4a7c15}
    // Send a typed `Work_Result` to the `Aggregator` actor using `ctx`
    actor.send(ctx, state.result_pool, req.reply_to, res)
}

aggregator_receive :: proc(ctx: ^actor.Context, state: ^Aggregator, res: ^Work_Result) {
    state.received += 1
}
```

> See [`examples/02_heterogeneous_pipeline.odin`](examples/02_heterogeneous_pipeline.odin) for a full multi-stage heterogeneous pipeline.

---

### 5. Wait-Free `Sharded_SPSC_Mailbox` (`827M msgs/sec`)

When an actor receives from a known set of $P$ producer threads (`0 ..< num_producers`), [`actor.Sharded_SPSC_Mailbox`](actor/spsc_mailbox.odin) gives each producer its own cache-line-isolated Single-Producer Single-Consumer ring buffer—**zero `LOCK XCHG` or `LOCK CMPXCHG` instructions on the data path**:

```odin
package main

import "core:fmt"
import "actor"

Sensor_Event :: struct {
    sensor_id: u32,
    reading:   f64,
}

main :: proc() {
    mbox: actor.Sharded_SPSC_Mailbox(Sensor_Event, 1024)
    actor.init(&mbox, num_producers = 4)
    defer actor.destroy(&mbox)

    // Producer 0 and Producer 1 push concurrently with zero atomic RMW contention
    actor.push(&mbox, producer_id = 0, val = Sensor_Event{sensor_id = 1, reading = 42.5})
    actor.push(&mbox, producer_id = 1, val = Sensor_Event{sensor_id = 2, reading = 99.1})

    // Single consumer drains up to 64 events across all producer lanes
    total := 0.0
    drained := actor.sharded_spsc_consume(&mbox, max_per_lane = 64, state = &total,
        handler = proc(total: ^f64, ev: ^Sensor_Event) {
            total^ += ev.reading
        },
    )
    fmt.printf("Drained %d events, sum = %.1f\n", drained, total)
}
```

> See [`examples/03_sharded_spsc_mailbox.odin`](examples/03_sharded_spsc_mailbox.odin) for a multi-threaded producer-consumer example.

---

### 6. Custom Allocators & Leak-Free Verification

Every `actor.init`, `actor.spawn`, and `actor.send` accepts an optional `allocator := context.allocator`, making it trivial to verify zero memory leaks with Odin's `mem.Tracking_Allocator` or run actors inside a fixed arena:

```odin
track: mem.Tracking_Allocator
mem.tracking_allocator_init(&track, context.allocator)
defer mem.tracking_allocator_destroy(&track)
alloc := mem.tracking_allocator(&track)

sched: actor.Scheduler
actor.init(&sched, num_workers = 4, allocator = alloc)
actor.start(&sched, allocator = alloc)

pid := actor.spawn(&sched, My_State{}, my_handler, allocator = alloc)
actor.send(&sched, pid, My_Msg{id = 1}, allocator = alloc)

// Shutdown and free all actors, queues, and worker threads
actor.destroy(&sched)
assert(len(track.allocation_map) == 0)
```

---

## API Reference Summary

| Procedure / Type | Description |
| :--- | :--- |
| [`actor.Scheduler`](actor/scheduler.odin) | Work-stealing `M:N` actor scheduler with per-worker Chase-Lev deques and adaptive `sync.Futex` parking. |
| [`actor.Envelope_Pool(Msg)`](actor/pool.odin) | Sharded watermark ring slab + per-shard intrusive return stack for $O(1)$ lock-free message recycling. |
| [`actor.Sharded_SPSC_Mailbox(T, N)`](actor/spsc_mailbox.odin) | Wait-free cache-line-isolated SPSC ring lanes (`1.2 ns/msg`, zero atomic RMW instructions). |
| [`actor.init`](actor/actor.odin) / [`actor.destroy`](actor/actor.odin) | Overloaded lifecycle procedures for `Scheduler`, `Envelope_Pool`, `MPSC_Queue`, and `Sharded_SPSC_Mailbox`. |
| [`actor.spawn`](actor/scheduler.odin) | Allocates and registers a typed actor `(State, Msg)` on a `Scheduler`, returning a type-safe `PID(Msg)`. |
| [`actor.send`](actor/actor.odin) | Sends a single `Msg` or batched `[]Msg` (`Chain=K`) via `Envelope_Pool` or heap allocator. |
| [`actor.forward`](actor/actor.odin) | Zero-copy `iso` forwarding of the currently executing message envelope to another actor of the same `Msg` type. |

---

## Benchmarks

Curious how Sleipnir (`package actor`) compares against the **official Pony language C runtime (`libponyrt` 0.77.2)**, **raw C11 (`stdatomic.h` + `pthreads`)**, **Odin `core:sync/chan`**, and **Data-Oriented Design (DOD) batch arrays**?

Read the full [**Benchmark Suite & Cross-Runtime Analysis (`benchmarks/README.md`)**](benchmarks/README.md).
