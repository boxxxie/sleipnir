# Sleipnir (`package actor`)

High-throughput, zero-GC, Pony-style actor runtime for [Odin](https://odin-lang.org/).

Sleipnir combines Vyukov Intrusive MPSC mailboxes, a sharded work-stealing `M:N` thread scheduler, lock-free `Envelope_Pool` slab recycling, and a wait-free `Sharded_SPSC_Mailbox`—all integrated with Odin's `context.allocator` and ergonomic compile-time polymorphism.

- **Zero Garbage Collector:** Deterministic memory management via automatic per-message `Envelope_Pool(Msg)` slabs or custom `mem.Allocator`.
- **Zero-Ceremony Ergonomics:** Single-line RAII `actor.scoped_scheduler` (`@(deferred_out)`), ambient thread-local context (no threading `&sched` or `ctx` through every call), decoupled `actor.PID(Msg)` handles, and multi-behavior tagged `union` handlers.
- **Performance:** **31–78M msgs/sec** on 4-producer MPSC fan-in, **827M msgs/sec** (`1.2 ns/msg`) on wait-free sharded SPSC, and **16.65M hops/sec** across 1,024 autonomous actors on 8 cores—matching or outperforming the official Pony C runtime (`libponyrt` 0.77.2) and raw C11 (`stdatomic.h`). See the full [**Benchmark Report & Cross-Runtime Comparison (`benchmarks/README.md`)**](benchmarks/README.md).

---

## Quick Start

Import `./actor` into your Odin project and run tests or examples:

```bash
odin test ./actor
odin run examples/01_ping_pong.odin -file -o:speed
odin run examples/02_heterogeneous_pipeline.odin -file -o:speed
odin run examples/03_sharded_spsc_mailbox.odin -file -o:speed
odin run examples/04_ergonomic_dsl.odin -file -o:speed
```

---

## Examples

### 1. Basic Actor (`scoped_scheduler` + Auto-Pooled `send`)

[`actor.scoped_scheduler`](actor/actor.odin) initializes and starts worker threads and automatically waits for idle and frees all spawned actors and pools when the scope exits (`@(deferred_out)`):

```odin
package main

import "core:fmt"
import "actor"

Counter :: struct {
    total: int,
}

on_add :: proc(self: ^Counter, amount: ^int) {
    self.total += amount^
}

main :: proc() {
    // Starts 2 worker threads and auto-destroys actors, pools, and threads at scope exit
    _ = actor.scoped_scheduler(num_workers = 2)

    counter := actor.spawn(Counter{}, on_add)

    for i in 1 ..= 1000 {
        actor.send(counter, i) // Pooled automatically via shared `Envelope_Pool(int)`!
    }

    actor.wait_messages(1000)
    actor.wait_idle()
    fmt.println("Total:", counter.state.total) // Prints: Total: 500500
}
```

---

### 2. Multi-Behavior Actors via Tagged `union` & `PID(Msg)`

In Pony, an actor can define multiple `be` behaviors. In Sleipnir, pass a tagged `union` by value (`proc(self: ^State, msg: Msg)`) and use [`actor.PID(Msg)`](actor/actor.odin) (`actor.pid(a)`) to address actors without exposing their private `$State` type:

```odin
Deposit  :: struct { amount: i64 }
Withdraw :: struct { amount: i64 }
Query    :: struct { reply_to: actor.PID(i64) }

Account_Msg :: union { Deposit, Withdraw, Query }

Bank_Account :: struct { balance: i64 }

account_behavior :: proc(self: ^Bank_Account, msg: Account_Msg) {
    switch m in msg {
    case Deposit:  self.balance += m.amount
    case Withdraw: self.balance -= m.amount
    case Query:    actor.send(m.reply_to, self.balance) // Ambient send inside handler!
    }
}
```

> See [`examples/04_ergonomic_dsl.odin`](examples/04_ergonomic_dsl.odin) for the complete runnable example (including `actor.spawn_self` compile-time structural typing).

---

### 3. Batched Message Splicing (`Chain=K`)

When a producer emits multiple messages to the same actor, passing a slice `[]Msg` to [`actor.send`](actor/actor.odin) links all $K$ envelopes locally and splices the entire chain into the target actor's mailbox with **a single atomic `XCHG`** (reducing atomic contention by $K\times$):

```odin
batch := [4]Account_Msg{
    Deposit{amount = 100},
    Deposit{amount = 50},
    Withdraw{amount = 25},
    Deposit{amount = 20},
}

// Links all 4 envelopes locally and performs 1 atomic XCHG on `account`'s mailbox
actor.send(account, batch[:])
```

---

### 4. Zero-Copy `iso` Message Forwarding (`actor.forward`)

An isolated (`iso`) message envelope can be mutated in place and forwarded to another actor without allocating or freeing a new envelope. Calling [`actor.forward`](actor/actor.odin) inside a handler transfers ownership of the current envelope directly to the next actor:

```odin
Ball_Msg :: struct {
    bounces:        i64,
    target_bounces: i64,
}

Table_State :: struct {
    partner: actor.PID(Ball_Msg),
}

player_receive :: proc(self: ^Table_State, msg: ^Ball_Msg) {
    if msg.bounces < msg.target_bounces {
        msg.bounces += 1
        // Mutate `msg` in place and forward the existing envelope with zero allocation!
        actor.forward(self.partner)
    } else {
        actor.complete_token()
    }
}
```

> See [`examples/01_ping_pong.odin`](examples/01_ping_pong.odin) for the complete runnable ping-pong example.

---

### 5. Wait-Free `Sharded_SPSC_Mailbox` (`827M msgs/sec`)

When an actor receives from a known set of $P$ producer threads (`0 ..< num_producers`), [`actor.Sharded_SPSC_Mailbox`](actor/spsc.odin) gives each producer its own cache-line-isolated Single-Producer Single-Consumer ring buffer—**zero `LOCK XCHG` or `LOCK CMPXCHG` instructions on the data path**:

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
    actor.push(&mbox, prod_id = 0, item = Sensor_Event{sensor_id = 1, reading = 42.5})
    actor.push(&mbox, prod_id = 1, item = Sensor_Event{sensor_id = 2, reading = 99.1})

    // Single consumer drains up to 64 events per lane across all producer lanes
    total := 0.0
    drained := actor.sharded_spsc_consume(&mbox, max_per_lane = 64, ctx = &total,
        handler = proc(total: ^f64, ev: ^Sensor_Event) {
            total^ += ev.reading
        },
    )
    fmt.printf("Drained %d events, sum = %.1f\n", drained, total)
}
```

> See [`examples/03_sharded_spsc_mailbox.odin`](examples/03_sharded_spsc_mailbox.odin) for a multi-threaded producer-consumer example.

---

### 6. Explicit Pools & Custom Allocators (`mem.Tracking_Allocator`)

For ultra-low-latency benchmarks or custom arenas, you can manage [`actor.Scheduler`](actor/actor.odin) and [`actor.Envelope_Pool`](actor/pool.odin) explicitly and verify zero memory leaks with `mem.Tracking_Allocator`:

```odin
track: mem.Tracking_Allocator
mem.tracking_allocator_init(&track, context.allocator)
defer mem.tracking_allocator_destroy(&track)
context.allocator = mem.tracking_allocator(&track)

{
    _ = actor.scoped_scheduler(num_workers = 4)
    a := actor.spawn(Counter{}, on_add)
    actor.send(a, 42)
    actor.wait_messages(1)
}

assert(len(track.allocation_map) == 0) // Zero bytes leaked!
```

---

## API Reference Summary

| Procedure / Type | Description |
| :--- | :--- |
| [`actor.scoped_scheduler`](actor/actor.odin) | Single-line RAII `M:N` scheduler (`@(deferred_out = scheduler_scope_end)`). |
| [`actor.Scheduler`](actor/actor.odin) | Work-stealing `M:N` actor scheduler with per-worker shards and adaptive `sync.Futex` parking. |
| [`actor.Actor(State, Msg)`](actor/actor.odin) / [`actor.PID(Msg)`](actor/actor.odin) | Concrete typed actor and state-erased `PID(Msg)` address handle (`actor.pid(a)`). |
| [`actor.Envelope_Pool(Msg)`](actor/pool.odin) | `snmalloc`-inspired batched envelope recycling pool (auto-managed per `Msg` or explicit). |
| [`actor.Sharded_SPSC_Mailbox(T, N)`](actor/spsc.odin) | Wait-free cache-line-isolated SPSC ring lanes (`1.2 ns/msg`, zero atomic RMW instructions). |
| [`actor.spawn`](actor/actor.odin) / [`actor.spawn_self`](actor/actor.odin) | Spawns an actor with 2-arg `(^State, ^Msg)`, 2-arg value `(^State, Msg)`, 3-arg `(^Context, ...)`, or embedded `receive` struct field. |
| [`actor.send`](actor/doc.odin) | Sends a single `Msg` or batched `[]Msg` (`Chain=K`) using ambient context or explicit scheduler/pool. |
| [`actor.forward`](actor/actor.odin) | Zero-copy `iso` forwarding of the currently executing message envelope (`actor.forward(pid)` or `actor.forward(pid, new_msg)`). |

---

## Benchmarks

Curious how Sleipnir (`package actor`) compares against the **official Pony language C runtime (`libponyrt` 0.77.2)**, **raw C11 (`stdatomic.h` + `pthreads`)**, **Odin `core:sync/chan`**, and **Data-Oriented Design (DOD) batch arrays**?

Read the full [**Benchmark Suite & Cross-Runtime Analysis (`benchmarks/README.md`)**](benchmarks/README.md).

---

## Acknowledgements & Prior Art

* **[Pony Language & `libponyrt`](https://github.com/ponylang/ponyc)**: For the intrusive MPSC mailbox design (`libponyrt/actor/messageq.c`), `pony_sendv` chain splicing, M:N cooperative actor scheduling, and `iso` reference capability semantics.
* **[Microsoft `snmalloc`](https://github.com/microsoft/snmalloc)**: For the batched cross-thread message return queue design that eliminates per-message cache-line invalidation during remote deallocation.
* **[Dmitry Vyukov](https://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue)**: For the foundational intrusive MPSC node-based queue algorithm.

---

## License

Released under the [MIT License](LICENSE).
