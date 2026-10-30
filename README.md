# Sleipnir (`package actor`) — Lock-Free Pony-Style M:N Actors for Odin

[![Odin](https://img.shields.io/badge/Odin-dev--2026-3882D2?logo=odin&logoColor=white)](https://odin-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> **Sleipnir** — Odin's legendary eight-legged steed (*"Odin's Pony"*).

**Sleipnir** (`package actor`) is a zero-dependency, pure-Odin concurrency library inspired by the **[Pony programming language](https://www.ponylang.io/)** (`libponyrt`) and Microsoft's **[snmalloc](https://github.com/microsoft/snmalloc)** message-passing allocator. It brings Pony's **lock-free intrusive MPSC actor mailboxes**, **batched message pool reclamation**, **zero-copy `iso` (isolated) message forwarding**, and **wait-free sharded SPSC ring mailboxes** directly to Odin.

---

## Why "Sleipnir"? (The Pony Connection)

The [Pony language](https://www.ponylang.io/) pioneered one of the fastest actor runtimes in systems programming (`libponyrt`) by combining:
1. **Intrusive lock-free MPSC actor mailboxes** (`libponyrt/actor/messageq.c`) where producers enqueue messages via a single atomic `XCHG` and consumers pop with zero atomic Read-Modify-Write instructions.
2. **Reference capabilities (`iso` / isolated pointers)** that allow mutable message payloads to be transferred across actors with **zero copying** and **zero data races**.
3. **Cooperative M:N work-stealing scheduling** where actors execute up to a fixed batch quantum of messages before yielding the worker thread.

**Sleipnir** (Odin's mythological steed—literally *"Odin's Pony"*) adapts Pony's runtime architecture and `snmalloc`'s batched cross-thread memory return into idiomatic, generic, zero-dependency Odin (`base:intrinsics`, `core:sync`, `core:thread`, and `core:mem`).

---

## Features

* **Heterogeneous M:N Work-Stealing Scheduler (`actor.Scheduler`)**
  Multiplexes thousands of lightweight, strongly-typed `actor.Actor($State, $Msg)` instances onto a configurable pool of OS worker threads with per-worker run-queue shards and work-stealing.
* **Lock-Free Intrusive MPSC Mailboxes (`actor.MPSC_Queue`)**
  Single-instruction atomic `XCHG` message enqueue (`mpsc_push`) and `pony_sendv`-style batch chain splicing (`mpsc_push_chain` / `send_batch_from_pool`). Supports both detached Vyukov pop (`mpsc_pop_detached`) and Pony's zero-RMW dummy-tail pop (`mpsc_pop_dummy`).
* **`snmalloc`-Style Batched Envelope Recycling (`actor.Envelope_Pool`)**
  Eliminates cross-core per-message cache-line invalidations by grouping consumed message envelopes by their origin pool during an actor's batch quantum and returning the entire linked chain in **one single atomic `XCHG`**.
* **Zero-Copy `iso` Ownership Forwarding (`actor.forward`)**
  Allows an actor to mutate and forward the currently executing message envelope directly to another actor's mailbox with **zero allocations and zero frees**.
* **Wait-Free Sharded SPSC Mailboxes (`actor.Sharded_SPSC_Mailbox`)**
  Replaces shared MPSC atomic exchanges with one cache-aligned ring lane per producer thread—reaching **750–900+ million msgs/sec (~1.1 ns/msg)** with zero `lock xchg` instructions.
* **Adaptive 2-Phase `sync.Futex` Parking (`enable_parking = true`)**
  Spins briefly with `intrinsics.cpu_relax()` under bursty traffic before sleeping idle worker threads on a kernel `Futex` for **0% idle CPU usage**, protected by a `.Seq_Cst` Dekker barrier to prevent lost wakeups.
* **First-Class Custom Allocator Support (`context.allocator`)**
  Every allocation point accepts an explicit `mem.Allocator` and is verified leak-free under Odin's `mem.Tracking_Allocator`.

---

## Repository Structure

```text
sleipnir/
├── actor/                              # Reusable library package (`package actor`)
│   ├── doc.odin                        # Package docs (`odin doc ./actor`) & overload groups
│   ├── mpsc.odin                       # Intrusive lock-free MPSC queue & RunQ_Shard
│   ├── spsc.odin                       # Wait-free SPSC_Lane & Sharded_SPSC_Mailbox
│   ├── pool.odin                       # Envelope_Pool (chain return) & Watermark_Slab
│   ├── actor.odin                      # Actor($State, $Msg), Scheduler, spawn/send/forward
│   └── actor_test.odin                 # Unit test suite (`odin test ./actor`)
├── examples/                           # Standalone runnable examples
│   ├── 01_ping_pong.odin               # Minimal 2-actor zero-copy ping-pong
│   ├── 02_heterogeneous_pipeline.odin  # Multiple actor types on one M:N scheduler
│   ├── 03_sharded_spsc_mailbox.odin    # Wait-free Sharded_SPSC_Mailbox fan-in
│   └── library_demo.odin               # Combined demo & benchmark verification
└── benchmarks/                         # Comparative Odin & C11 benchmark suite
    ├── actor_benchmark.odin            # Full 5-workload comparative Odin benchmark suite
    ├── actor_benchmark_c11.c           # Matching C11 / pthreads reference benchmark
    └── benchmark_results_and_findings.md # Architectural deep-dive & empirical findings
```

---

## Installation

Because **Sleipnir** has zero external dependencies beyond Odin's `base:` and `core:` collections, you can add it to any Odin project in two ways:

### Option 1: Copy or Submodule into Your Project
```bash
git submodule add https://github.com/boxxxie/sleipnir.git third_party/sleipnir
```
Then import it directly via relative path:
```odin
import "third_party/sleipnir/actor"
```

### Option 2: Use an Odin Compiler Collection (`-collection`)
Pass `-collection:sleipnir=./path/to/sleipnir` (or `-collection:actor=./path/to/sleipnir/actor` as configured in `ols.json`):
```bash
odin run src/ -collection:sleipnir=./third_party/sleipnir -o:speed
```
```odin
import "sleipnir:actor"
```

---

## Quickstart

### 1. Heterogeneous Actors with Pooled Messages & Futex Parking

See [`examples/02_heterogeneous_pipeline.odin`](examples/02_heterogeneous_pipeline.odin):

```odin
package main

import "actor"
import "core:fmt"

Add_Msg :: struct {
	amount:   i64,
	reply_to: ^actor.Actor(Aggregator_State, i64),
}

Counter_State    :: struct { count: i64 }
Aggregator_State :: struct { total: i64 }

counter_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Counter_State, Add_Msg), msg: ^Add_Msg) {
	self.state.count += msg.amount
	if msg.reply_to != nil {
		// Send our subtotal to a completely different Actor type on the same scheduler:
		actor.send(ctx, msg.reply_to, self.state.count)
	}
}

aggregator_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Aggregator_State, i64), msg: ^i64) {
	self.state.total += msg^
	actor.complete_token(ctx, 1)
}

main :: proc() {
	sched: actor.Scheduler
	actor.init(&sched, actor.Scheduler_Config{
		num_workers    = 2,
		enable_parking = true, // 0% CPU when idle
	})
	defer actor.destroy(&sched)

	add_pools := make([]actor.Envelope_Pool(Add_Msg), 2)
	rep_pools := make([]actor.Envelope_Pool(i64), 2)
	for w := 0; w < 2; w += 1 {
		actor.init(&add_pools[w], u16(w), 128)
		actor.init(&rep_pools[w], u16(w), 128)
	}
	defer {
		for w := 0; w < 2; w += 1 {
			actor.destroy(&add_pools[w])
			actor.destroy(&rep_pools[w])
		}
		delete(add_pools)
		delete(rep_pools)
	}

	agg     := actor.spawn(Aggregator_State{}, aggregator_receive, rep_pools)
	counter := actor.spawn(Counter_State{}, counter_receive, add_pools)
	defer actor.destroy(agg)
	defer actor.destroy(counter)

	sched.pending_tokens = 1
	actor.scheduler_start(&sched)

	actor.send(&sched, &add_pools[0], counter, Add_Msg{amount = 20, reply_to = nil})
	actor.send(&sched, &add_pools[0], counter, Add_Msg{amount = 22, reply_to = agg})

	actor.scheduler_wait_tokens(&sched)
	fmt.printfln("Total = %d", agg.state.total) // Output: Total = 42
}
```

### 2. Zero-Copy `iso` Message Forwarding (`actor.forward`)

When messages circulate through an actor graph (e.g., state machines, pipelines, token rings, or large payloads), `actor.forward` transfers ownership of the active `Envelope($Msg)` directly to the next actor without touching any memory pool or allocator (see [`examples/01_ping_pong.odin`](examples/01_ping_pong.odin)):

```odin
player_receive :: proc(ctx: ^actor.Context, self: ^actor.Actor(Table_State, Ball_Msg), msg: ^Ball_Msg) {
	if msg.bounces < msg.target_bounces {
		// Reuses the active Envelope(Ball_Msg) in-place (zero allocation, zero free):
		actor.forward(ctx, self.state.partner, Ball_Msg{
			bounces        = msg.bounces + 1,
			target_bounces = msg.target_bounces,
		})
	} else {
		actor.complete_token(ctx, 1)
	}
}
```

### 3. Wait-Free Sharded SPSC Mailbox (`actor.Sharded_SPSC_Mailbox`)

When a known number of producer threads feed a hot actor, `Sharded_SPSC_Mailbox` eliminates `lock xchg` contention by giving each producer its own cache-aligned wait-free ring lane (see [`examples/03_sharded_spsc_mailbox.odin`](examples/03_sharded_spsc_mailbox.odin)):

```odin
mbox: actor.Sharded_SPSC_Mailbox(Event, 8192)
actor.init(&mbox, num_producers)
defer actor.destroy(&mbox)

// On Producer Thread `p`:
actor.push(&mbox, p, my_event)

// On Consumer Actor:
processed := actor.sharded_spsc_consume(&mbox, 128, &actor_state, proc(s: ^State, ev: ^Event) {
	// Process up to 128 events per lane with a single .Release index update per batch!
})
```

---

## Core API & Procedure Overload Groups

Following Odin's `core:` conventions, `package actor` provides both explicit procedure names and overloaded verb groups:

| Overload Group | Underlying Procedures | Description |
| :--- | :--- | :--- |
| **`actor.init`** | `scheduler_init`, `actor_init`, `pool_init`, `watermark_slab_init`, `mpsc_init`, `shard_init`, `sharded_spsc_init` | Initialize a scheduler, in-place actor, envelope pool, watermark slab, or queue. |
| **`actor.destroy`** | `scheduler_stop_and_destroy`, `actor_destroy`, `pool_destroy`, `watermark_slab_destroy`, `sharded_spsc_destroy` | Stop workers and free all memory associated with the resource. |
| **`actor.spawn`** | `spawn(initial_state, receive_proc, pools, id, allocator)` | Heap-allocate and initialize a typed `^Actor($State, $Msg)`. |
| **`actor.send`** | `send_ctx`, `send_from_pool`, `send_batch_from_pool`, `send_envelope`, `send_envelope_chain` | Send a single message, a pre-linked batch of messages (`Chain=K`), or a static envelope to an actor. |
| **`actor.forward`** | `forward(ctx, target_actor, updated_msg)` | Zero-copy `iso` ownership transfer of the active envelope to `target_actor`. |
| **`actor.push`** | `mpsc_push`, `shard_push`, `spsc_try_push`, `sharded_spsc_push` | Low-level lock-free MPSC or wait-free SPSC enqueue. |

---

## Benchmark Suite & Empirical Results

Run the full 5-workload benchmark suite (`odin run benchmarks/actor_benchmark.odin -file -o:speed`):

### Workload 1: 4-Producer $\rightarrow$ 1-Actor Fan-In Contention (2,000,000 Messages)

| Implementation | Total Time | Throughput | Latency / Msg | Speedup vs. `chan` |
| :--- | ---: | ---: | ---: | ---: |
| **3e. Sharded SPSC Ring Lanes (`Sharded_SPSC_Mailbox`)** | **2.31 ms** | **866.97 M/s** | **1.2 ns** | **624.0x** |
| **2a. DOD Batch Array (Single-Thread Baseline)** | 4.75 ms | 421.48 M/s | 2.4 ns | 303.4x |
| **3d. Pony MPSC (`mpsc_push_chain` Chain=32 Splice)** | 6.27 ms | 318.73 M/s | 3.1 ns | 229.9x |
| **2b. DOD Multi-Thread (4 TLS Buffers + Barrier Drain)** | 9.19 ms | 217.64 M/s | 4.6 ns | 156.8x |
| **5a. `package actor` (`send_batch_from_pool` Chain=32)** | **26.82 ms** | **74.56 M/s** | **13.4 ns** | **53.7x** |
| **3c. Pony MPSC (`Watermark_Slab` Batched FIFO 8K)** | 58.88 ms | 33.97 M/s | 29.4 ns | 24.5x |
| **3b. Pony MPSC (Per-Message `in_use` 8K Slab)** | 61.21 ms | 32.67 M/s | 30.6 ns | 23.5x |
| **5a. `package actor` (`Envelope_Pool` Single Send)** | **67.83 ms** | **29.49 M/s** | **33.9 ns** | **21.2x** |
| **3f. Pony MPSC + Adaptive Futex Parker (`sync.Futex`)** | 68.73 ms | 29.10 M/s | 34.4 ns | 21.0x |
| **1. Traditional (`core:sync/chan` + `core:thread`)** | 1,441.32 ms | 1.39 M/s | 720.7 ns | 1.0x |

### Workloads 2–5 Summary

* **Workload 2 (Cross-Thread 1-to-1 Ping-Pong, 200,000 Hops):** Lock-free Pony MPSC completes in **24.22 ms (121.1 ns/hop)** vs. `core:sync/chan` at **968.33 ms (4,841.6 ns/hop)** (**40x faster**).
* **Workload 3 (512-Byte Large Payload Zero-Copy `iso` Transfer, 500,000 Messages):** Completes in **14.63 ms (34.19 M/s, 29.3 ns/msg)** by passing 8-byte pointers in-place rather than copying 512B payloads into ring buffers.
* **Workload 4 (1,024-Actor Work-Stealing Swarm, 2,000,000 Messages):** `package actor` M:N Swarm processes 2M messages across 1,024 actors in **45.80 ms (43.66 M/s, 22.9 ns/msg)**.
* **Workload 5 (Autonomous 1,024-Actor Graph + 64 Rounds of SplitMix64 CPU Work/Hop, 500,000 Hops):** Scales linearly from **163.10 ms (3.07 M hops/s)** on 1 worker to **29.25 ms (17.09 M hops/s)** on 8 workers using `actor.forward`.

For a detailed write-up of the seven key architectural lessons learned while building this suite, see [`benchmarks/benchmark_results_and_findings.md`](benchmarks/benchmark_results_and_findings.md).

---

## Testing & Documentation

Run the unit test suite (which validates all queues, pools, slabs, and schedulers under `mem.Tracking_Allocator` for zero memory leaks):

```bash
odin test ./actor
```

Generate package documentation using `odin doc`:

```bash
odin doc ./actor
```

Run the standalone examples:

```bash
odin run examples/01_ping_pong.odin -file -o:speed
odin run examples/02_heterogeneous_pipeline.odin -file -o:speed
odin run examples/03_sharded_spsc_mailbox.odin -file -o:speed
odin run examples/library_demo.odin -file -o:speed
```

---

## Acknowledgements & Prior Art

* **[Pony Language & `libponyrt`](https://github.com/ponylang/ponyc)**: For the intrusive MPSC mailbox design (`libponyrt/actor/messageq.c`), `pony_sendv` chain splicing, M:N cooperative actor scheduling, and `iso` reference capability semantics.
* **[Microsoft `snmalloc`](https://github.com/microsoft/snmalloc)**: For the batched cross-thread message return queue design that eliminates per-message cache-line invalidation during remote deallocation.
* **[Dmitry Vyukov](https://www.1024cores.net/home/lock-free-algorithms/queues/intrusive-mpsc-node-based-queue)**: For the foundational intrusive MPSC node-based queue algorithm.

---

## License

Released under the [MIT License](LICENSE).
