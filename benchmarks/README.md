# Sleipnir Benchmark Suite & Microarchitectural Analysis

This directory contains the comparative benchmark suite for **Sleipnir (`package actor`)** alongside standalone hand-rolled Odin baselines, the **official Pony runtime (`libponyrt` 0.77.2)** linked via Odin C FFI, and a 1:1 C11 (`stdatomic.h` + `pthreads`) reference implementation:

* **[`actor_benchmark.odin`](actor_benchmark.odin)**: Complete 5-workload Odin benchmark suite comparing `core:sync/chan`, Data-Oriented Design (DOD) batching, hand-rolled Pony MPSC variants (`3b`–`3h`), wait-free Sharded SPSC lanes (`3e`), the official **Pony runtime (`libponyrt` 0.77.2)** (`4a`–`4e`, enabled via `-define:USE_LIBPONYRT=true`), and the reusable **Sleipnir (`package actor`)** library (`5a`–`5c`).
* **[`build_libponyrt.sh`](build_libponyrt.sh)**: Automated build script that shallow-clones `ponylang/ponyc` (`0.77.2`) into `.tmp/ponyc_src` and compiles `.tmp/ponyc_build/libponyrt.a` and `libponyrt.so` with `gcc -O3 -march=native` in ~6 seconds.
* **[`actor_benchmark_c11.c`](actor_benchmark_c11.c)**: Equivalent C11 / `pthreads` reference implementation of Workloads 1 and 5 used to verify compiler code generation and hardware parity.

---

## 1. Reference System Configuration

All benchmark measurements in this document were captured on the following bare-metal Linux system:

| Component | Specification | Microarchitectural Relevance |
| :--- | :--- | :--- |
| **Processor** | **AMD Ryzen 7 7735U** (Zen 3+ / Rembrandt) | 8 Physical Cores / 16 Threads (2-way SMT), up to **4.82 GHz** boost |
| **Core / CCX Topology** | **Single 8-Core CCX** (1 Socket, 1 NUMA Node) | All 8 physical cores share a unified L3 cache; uniform core-to-core (C2C) atomic transfer latency |
| **L1 Cache** | **256 KiB L1d** (8 $\times$ 32 KiB) + **256 KiB L1i** | 64-byte cache line size; private per physical core (shared across the 2 SMT threads per core) |
| **L2 Cache** | **4 MiB Unified** (8 $\times$ 512 KiB, 8-way) | Private per physical core; absorbs 8K ring slabs (192 KB) without spilling to L3 |
| **L3 Cache** | **16 MiB Unified** (Shared across all 8 cores) | Handles cross-core cache-coherency (MOESI) probes for `LOCK XCHG` and `.Release`/`.Acquire` traffic |
| **System Memory** | **14 GiB RAM** (Single NUMA Domain, CPUs 0–15) | No cross-socket NUMA penalty |
| **Operating System** | **Manjaro Linux x86_64** (`Linux 7.1.13-2-MANJARO`) | Native Linux `futex(2)` support via `core:sync` (`sync.Futex`) |
| **Odin Compiler** | **`odin version dev-2026-09:a2fb372b7`** | Compiled with `-o:speed` (LLVM backend `-O3` equivalent) |
| **Pony Runtime** | **Official `ponylang/ponyc` `libponyrt` `0.77.2`** | Compiled from source with `gcc -O3 -march=native -DPONY_NDEBUG -DPOOL_USE_DEFAULT` |
| **C11 Compiler** | **`gcc (GCC) 16.2.1 20260810`** | Compiled with `-O3 -march=native -pthread` |

---

## 2. How to Run the Benchmarks

Run from the repository root:

```bash
# 1. Run the pure-Odin benchmark suite (zero external dependencies)
odin run benchmarks/actor_benchmark.odin -file -o:speed

# 2. Build official `libponyrt` (0.77.2) and run the full Odin + Pony C FFI suite
./benchmarks/build_libponyrt.sh
odin run benchmarks/actor_benchmark.odin -file -o:speed \
  -define:USE_LIBPONYRT=true -extra-linker-flags:"-latomic -lm -ldl"

# 3. Compile and run the C11 / pthreads reference suite (with or without `libponyrt`)
gcc -O3 -march=native -pthread benchmarks/actor_benchmark_c11.c -o .tmp/actor_benchmark_c11
./.tmp/actor_benchmark_c11

gcc -O3 -march=native -pthread -DUSE_LIBPONYRT \
  -I.tmp/ponyc_src/src/libponyrt -I.tmp/ponyc_src/src/common \
  benchmarks/actor_benchmark_c11.c .tmp/ponyc_build/libponyrt.a \
  -latomic -lm -ldl -o .tmp/actor_benchmark_c11_pony
./.tmp/actor_benchmark_c11_pony
```

---

## 3. Empirical Benchmark Results (Workloads 1–5)

### Workload 1: 4-Producer $\rightarrow$ 1-Actor Fan-In Contention (2,000,000 Messages, 8B Payload)

**What it stresses:** 4 concurrent producer threads saturating a single actor's mailbox with 2,000,000 messages (500,000 messages/producer). Isolates multi-producer queue contention (`LOCK XCHG` vs. `.Release` stores), cross-thread memory slab/pool reclamation, and MOESI cache-line invalidation traffic.

#### Odin + Official `libponyrt` 0.77.2 Results (`benchmarks/actor_benchmark.odin`)

| ID | Architecture & Memory Reclamation Strategy | Total Time | Throughput | Latency / Msg | Speedup vs. `chan` |
| :--- | :--- | ---: | ---: | ---: | ---: |
| **`3e`** | **Sharded SPSC Ring Lanes (4 $\times$ 8K Wait-Free Rings, Zero RMW)** | **2.42 ms** | **827.45 M/s** | **1.2 ns** | **624.1x** |
| **`2a`** | **DOD Batch Array (Single-Thread Sequential Baseline)** | 5.16 ms | 387.54 M/s | 2.6 ns | 292.7x |
| **`3d`** | **Hand-Rolled Pony MPSC (`mpsc_push_chain` Chain=32, 8K Slab)** | 6.37 ms | 313.83 M/s | 3.2 ns | 237.1x |
| **`2b`** | **DOD Multi-Thread (4 Disjoint TLS Buffers + Barrier Drain)** | 10.12 ms | 197.70 M/s | 5.1 ns | 149.2x |
| **`5a`** | **Sleipnir `package actor` (`send_batch_from_pool` Chain=32, `Envelope_Pool`)** | **25.68 ms** | **77.89 M/s** | **12.8 ns** | **58.8x** |
| **`4b`** | **Official Pony (`libponyrt` 0.77.2 `pony_alloc_msg` + `pony_chain` Chain=32)** | **27.74 ms** | **72.10 M/s** | **13.9 ns** | **54.4x** |
| **`3c`** | **Hand-Rolled Pony MPSC (`Watermark_Slab` Batched FIFO 8K)** | 59.63 ms | 33.54 M/s | 29.8 ns | 25.3x |
| **`3b`** | **Hand-Rolled Pony MPSC (Per-Message `in_use` Flag 8K Slab)** | 60.90 ms | 32.84 M/s | 30.4 ns | 24.8x |
| **`5a`** | **Sleipnir `package actor` (`send_from_pool` Single Send, `Envelope_Pool`)** | **64.36 ms** | **31.08 M/s** | **32.2 ns** | **23.5x** |
| **`3f`** | **Hand-Rolled Pony MPSC + Adaptive 2-Phase `sync.Futex` Parker** | 69.46 ms | 28.79 M/s | 34.7 ns | 21.7x |
| **`4a`** | **Official Pony (`libponyrt` 0.77.2 `pony_alloc_msg` + `messageq_push` Single)** | **85.32 ms** | **23.44 M/s** | **42.7 ns** | **17.7x** |
| **`1`** | **Traditional Actor (`core:sync/chan` Buffered 4096 + `core:thread`)** | 1,510.33 ms | 1.32 M/s | 755.2 ns | 1.0x |

> [!IMPORTANT]
> **Sleipnir (`package actor`) vs. Official Pony (`libponyrt` 0.77.2) on Fan-In:**
> * **Single-Message Send (`5a` vs. `4a`):** Sleipnir's `Envelope_Pool` (`64.36 ms` / `31.08 M/s`) is **1.33x faster** than official `libponyrt` 0.77.2 (`85.32 ms` / `23.44 M/s`). Both use the same `snmalloc`-inspired batched cross-thread return architecture (`BATCH_SIZE = 32` in `libponyrt/mem/pool_arena.c` vs. `Batch_Return_Collector` in `actor/pool.odin`), while Sleipnir avoids `libponyrt`'s general-purpose arena unit bitmap/slab header lookup by routing envelopes directly via `Envelope.pool_id`.
> * **Batched Chain=32 Send (`5a` vs. `4b`):** Sleipnir's `send_batch_from_pool` (`25.68 ms` / `77.89 M/s`) and official `libponyrt` `pony_chain` (`27.74 ms` / `72.10 M/s`) perform within **7% (`12.8 ns` vs. `13.9 ns/msg`)** of each other.

#### C11 / `pthreads` Reference Comparison (`benchmarks/actor_benchmark_c11.c`)

| ID | C11 Reference Implementation (`gcc -O3 -march=native`) | C11 Time | C11 Throughput | Odin Time | Odin Throughput |
| :--- | :--- | ---: | ---: | ---: | ---: |
| **`1`** | Traditional (`pthread_mutex_t` + `pthread_cond_t` Ring) | 438.28 ms | 4.56 M/s | 1,510.33 ms | 1.32 M/s |
| **`2a`** | DOD Batch Array (Single-Thread Auto-Vectorized) | 2.47 ms | 811.26 M/s | 5.16 ms | 387.54 M/s |
| **`2b`** | DOD Multi-Thread (4 TLS Buffers + Barrier Drain) | 6.41 ms | 311.77 M/s | 10.12 ms | 197.70 M/s |
| **`3c`** | Pony MPSC (Batched FIFO Watermark 8K Slab) | 59.94 ms | 33.37 M/s | **59.63 ms** | **33.54 M/s** |
| **`3d`** | Pony MPSC (`mpsc_push_chain` Chain=32 Splice 8K Slab) | 6.50 ms | 307.53 M/s | **6.37 ms** | **313.83 M/s** |
| **`3e`** | Sharded SPSC Ring Lanes (Zero RMW Atomics) | 3.56 ms | 561.76 M/s | **2.42 ms** | **827.45 M/s** |
| **`4a`** | Official Pony (`libponyrt` 0.77.2 Single Send) | 89.34 ms | 22.39 M/s | **85.32 ms** | **23.44 M/s** |
| **`4b`** | Official Pony (`libponyrt` 0.77.2 `pony_chain` Chain=32) | 27.00 ms | 74.07 M/s | **27.74 ms** | **72.10 M/s** |

---

### Workload 2: Cross-Thread 1-to-1 Ping-Pong Latency (100,000 Round-Trips = 200,000 Hops)

**What it stresses:** Pure inter-core cache-to-cache (C2C) round-trip wakeup and message transfer latency. Two actors pinned to separate OS threads bounce a single token back and forth 100,000 times sequentially (each hop depends causally on the previous hop).

| ID | Implementation | Total Time | Throughput | One-Way Hop Latency | Round-Trip Time (RTT) | Speedup |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **`3`** | **Pony Intrusive MPSC in Odin (`atomic_exchange` + Dummy-Tail Pop)** | **24.30 ms** | **8.23 M hops/s** | **121.5 ns** | **243.0 ns** | **41.5x** |
| **`4`** | **Official Pony (`libponyrt` 0.77.2 `pony_alloc_msg` + `messageq`)** | **38.48 ms** | **5.20 M hops/s** | **192.4 ns** | **384.8 ns** | **26.2x** |
| **`1`** | **Traditional (`core:sync/chan` Buffered + `core:thread`)** | 1,009.37 ms | 0.20 M hops/s | 5,046.8 ns | 10,093.7 ns | 1.0x |

---

### Workload 3: 512-Byte Large Payload Crossover (500,000 Messages, 4 Producers $\rightarrow$ 1 Actor)

**What it stresses:** Memory bandwidth vs. pointer ownership handoff. Each message carries a 512-byte payload (`[64]i64`, spanning 8 cache lines). In Pony MPSC (`3c`) and `libponyrt` (`4c`), the producer initializes the payload in-place inside its pool/slab slot and passes only an **8-byte pointer** (`iso` reference capability transfer):

| ID | Implementation | Total Time | Throughput | Latency / Msg | Effective Payload Bandwidth |
| :--- | :--- | ---: | ---: | ---: | ---: |
| **`3c`** | **Odin Pony MPSC Zero-Copy `iso` Pointer (512B Ring Slab)** | **14.76 ms** | **33.88 M/s** | **29.5 ns** | **17.35 GB/s equivalent** |
| **`4c`** | **Official Pony (`libponyrt` 0.77.2 512B Pool Arena)** | **44.92 ms** | **11.13 M/s** | **89.8 ns** | **5.70 GB/s equivalent** |

Notice that transferring a **512-byte** message via Odin Pony MPSC Zero-Copy (`29.5 ns/msg`) takes the **exact same time** as transferring an **8-byte** message (`29.8 ns/msg` in Workload 1 `3c`), because the MPSC queue only ever exchanges an 8-byte intrusive node pointer (`^Large_Pony_Msg`)!

---

### Workload 4: 1,024-Actor Swarm Scaling (2,000,000 Messages Across 1,024 Actors)

**What it stresses:** Multiplexing 1,024 independent actors onto 2 M:N worker threads while 4 concurrent external producer threads fire 2,000,000 messages across all 1,024 actors. Compares the specialized Odin swarm (`3g`), the full official **`libponyrt` 0.77.2 M:N runtime** (`4d`, using `pony_create` + `ponyint_sendi_inject` + `pony_start`), and the reusable **Sleipnir (`package actor`)** runtime (`5b`):

| ID | Implementation | Total Time | Throughput | Latency / Msg | Recycling & Scheduling Mechanism |
| :--- | :--- | ---: | ---: | ---: | :--- |
| **`3g`** | **Hand-Rolled Sharded Work-Stealing M:N Swarm (1,024 Actors)** | 37.07 ms | 53.95 M/s | 18.5 ns | Per-slot `aux_id` flag + 4 `RunQ_Shard`s |
| **`5b`** | **Sleipnir `package actor` Generic M:N Swarm (1,024 `Actor(S, M)`s)** | **41.85 ms** | **47.79 M/s** | **20.9 ns** | `Envelope_Pool` chain return + 4 `RunQ_Shard`s |
| **`4d`** | **Official Pony (`libponyrt` 0.77.2 Full M:N Runtime, 1,024 `pony_actor_t`s)** | **103.69 ms** | **19.29 M/s** | **51.8 ns** | `ponyint_sendi_inject` global `mpmcq_t` + `pool_arena` |

Why is **Sleipnir (`5b`, `47.79 M/s`) 2.48x faster than official `libponyrt` (`4d`, `19.29 M/s`)** when external threads inject messages into an actor swarm?
* In `libponyrt` (`sched/scheduler.c`), external C/Odin threads that are not Pony scheduler threads must push scheduled actors onto a single global MPMC inject queue (`ponyint_mpmcq_push(&inject, actor)`).
* In **Sleipnir (`package actor`)**, external producers can target sharded run queues (`sched.shards[shard_hint]`) and benefit from **Test-and-Test-and-Set (TTAS)** on `Actor_Base.scheduled`, avoiding a global MPMC bottleneck.

---

### Workload 5: Autonomous Actor-to-Actor Graph + Simulated CPU Work (500,000 Hops, 1,024 Actors)

**What it stresses:** A closed, autonomous network of 1,024 actors with **zero external producer threads**. 8,192 initial tokens circulate through the graph for 500,000 total hops. On every hop, the receiving actor:
1. Executes **64 rounds of SplitMix64** (~300 ns of non-trivial ALU work per message, modeling game AI, serialization, or protocol state machines) mixed with the actor's private state.
2. Mutates its internal 64-bit state (`self.state ~= res`).
3. Computes the next target actor deterministically from the hash (`target_id := (self.id * 31 + u32(res)) & 1023`).
4. Forwards the message to the target actor (`actor.forward` in Sleipnir vs. `pony_alloc_msg` + `pony_sendv` in `libponyrt` 0.77.2) and schedules it on the M:N work-stealing run queue.

| Worker Threads | `3h` Hand-Rolled Odin | `4e` Official Pony (`libponyrt` 0.77.2) | `5c` Sleipnir (`package actor`) | C11 `pthreads` Reference | Parallel Speedup (vs. 1 Worker) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **1 Worker (1 Core)** | 166.48 ms (3.00 M/s) | **179.28 ms (2.79 M/s)** | **168.80 ms (2.96 M/s)** | 161.11 ms (3.10 M/s) | **1.00x** (`337.6 ns/hop` vs. `358.6 ns/hop` Pony) |
| **2 Workers (2 Cores)** | 87.15 ms (5.74 M/s) | **96.60 ms (5.18 M/s)** | **86.00 ms (5.81 M/s)** | 83.13 ms (6.01 M/s) | **1.96x** (`172.0 ns/hop` vs. `193.2 ns/hop` Pony) |
| **4 Workers (4 Cores)** | 45.64 ms (10.96 M/s) | **48.94 ms (10.22 M/s)** | **47.53 ms (10.52 M/s)** | 44.11 ms (11.34 M/s) | **3.55x** (`95.1 ns/hop` vs. `97.9 ns/hop` Pony) |
| **8 Workers (8 Physical Cores)** | 26.74 ms (18.70 M/s) | **25.38 ms (19.70 M/s)** | **30.03 ms (16.65 M/s)** | 24.02 ms (20.82 M/s) | **5.62x–7.06x** (`50.8–60.1 ns/hop`) |

> [!TIP]
> **Parity with Official `libponyrt` 0.77.2 Across 1–8 Physical Cores:**
> * On 1, 2, and 4 worker threads, **Sleipnir (`package actor`)** slightly outperforms official `libponyrt` 0.77.2 (`168.80 ms` vs. `179.28 ms` at 1T; `86.00 ms` vs. `96.60 ms` at 2T; `47.53 ms` vs. `48.94 ms` at 4T) because `actor.forward` reuses the active `Envelope($Msg)` in-place (`iso` token forwarding) without calling `pony_alloc_msg` + `ponyint_pool_free` on every hop.
> * At 8 worker threads (saturating all 8 physical Zen 3+ cores), `libponyrt` (`25.38 ms` / `19.70 M/s`), `3h` Hand-Rolled Odin (`26.74 ms` / `18.70 M/s`), and Sleipnir `package actor` (`30.03 ms` / `16.65 M/s`) converge within a few nanoseconds per hop (`50.8 ns` to `60.1 ns/hop`).
> * Note that `libponyrt` enforces `--ponymaxthreads <= physical_cores` (`8` on the Ryzen 7 7735U) in `sched/start.c` (`"Can't have --ponymaxthreads > physical cores"`), because spawning more worker threads than physical cores on CPU-bound actor workloads causes SMT ALU contention and spin-lock preemption.

---

## 4. Deep-Dive Engineering & Microarchitectural Findings

### 4.1. Cross-Thread Memory Reclamation vs. Queue Contention (`snmalloc` & `libponyrt` 0.77.2 Arena Principle)
In an intrusive MPSC queue (`mpsc_push`), enqueueing a node requires only a single atomic pointer swap (`XCHG` on `q.head`) and a release store (`prev.next = node`). However, in bounded-memory systems, **reclaiming** that message node across threads is often more expensive than enqueueing it:
* If the consumer clears a per-message flag (`tail.in_use = 0`) on every pop, the consumer core must transition every single message cache line into the `Modified (M)` state, invalidating the producer's L1/L2 cache line 2,000,000 times.
* Both **official `libponyrt` 0.77.2** (`src/libponyrt/mem/pool_arena.c`) and **Sleipnir (`actor/pool.odin`)** solve this using `snmalloc`-style batched cross-thread return queues:
  * In `libponyrt` 0.77.2, foreign-freed blocks accumulate in per-owner thread chains and flush at `BATCH_SIZE = 32` to the owner's cache-line-aligned `inbox_t` with a single atomic CAS.
  * In **Sleipnir (`package actor`)**, each producer/worker owns a local `Envelope_Pool($Msg)`. During an actor's batch quantum (e.g., 64 or 100 messages), retired envelopes are grouped by their `pool_id` into a stack-allocated `Batch_Return_Collector` linked list and returned to the origin pool's `remote_returns` queue in **one single `mpsc_push_chain` (`XCHG`) per batch**.

### 4.2. Pony's Dummy-Tail Pop (`mpsc_pop_dummy`) vs. Vyukov's Detached Pop (`mpsc_pop_detached`)
`actor/mpsc.odin` provides two distinct consumer pop algorithms for `MPSC_Queue($T)`:
1. **`mpsc_pop_dummy` (Zero Consumer RMW Atomics):**
   Used by `libponyrt` (`libponyrt/actor/messageq.c`). Instead of re-inserting a sentinel node when the queue empties, the consumer leaves the **most recently popped message node** in place as `q.tail`.
   * *Advantage:* The consumer never touches `q.head` and executes **zero atomic RMW instructions** (only an `.Acquire` load on `q.tail.next`).
   * *Constraint:* Because the last popped node remains pointed to by `q.tail` until the *next* message arrives at that actor, that node cannot be immediately freed or forwarded (`iso`) to another actor.
2. **`mpsc_pop_detached` (Vyukov Sentinel Re-Insertion — Default in `Actor`):**
   When `q.tail` reaches the last element (`tail == head` and `tail.next == nil`), the consumer re-inserts the queue's embedded `q.stub` sentinel via `atomic_exchange(&q.head, &q.stub)`.
   * *Advantage:* The returned `^Envelope(Msg)` is **100% detached** from the actor's mailbox the instant `mpsc_pop_detached` returns. This enables both immediate pool reclamation (even if the actor goes idle forever) and **zero-copy `actor.forward` (`iso`)** across arbitrary multi-actor graphs.

### 4.3. Test-and-Test-and-Set (TTAS) on `Actor_Base.scheduled`
When multiple producers send messages to a hot actor (`send_envelope`), the scheduler must ensure the actor is queued on a `RunQ_Shard` if it isn't already running.
* Calling `atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel)` unconditionally on every `send` forces a `LOCK XCHG` on the actor's base cache line on every message, even when `a.scheduled` is already `1`.
* Instead, `scheduler_schedule_actor` in [`actor/actor.odin`](../actor/actor.odin) performs a `.Seq_Cst` load first:
  ```odin
  if intrinsics.atomic_load_explicit(&a.scheduled, .Seq_Cst) == 0 {
      if intrinsics.atomic_exchange_explicit(&a.scheduled, 1, .Acq_Rel) == 0 {
          // Push to RunQ_Shard...
      }
  }
  ```
  While an actor is actively scheduled (`a.scheduled == 1`), its cache line remains in the **Shared (`S`)** state in L1 cache across all producer cores, turning the schedule check into a 1-cycle L1 read and improving 4-producer fan-in speed by **~32%** (`97.38 ms` $\rightarrow$ `64.36 ms`).

### 4.4. Lock-Free Dekker Wake/Unschedule Protocol
To prevent the classic lost-wakeup race—where Producer A pushes a message and sees `a.scheduled == 1` just as Worker B finishes draining the mailbox and sets `a.scheduled = 0`—`scheduler_worker_proc` and `mpsc_is_empty` enforce a `.Seq_Cst` store-load Dekker barrier:
1. Producer pushes message to `target.mailbox.head` (`XCHG`, implicit full barrier on x86_64) and loads `target.scheduled` (`.Seq_Cst`).
2. Worker drains mailbox, stores `a.scheduled = 0` (`.Seq_Cst`), and immediately re-probes `!mpsc_is_empty(&self.mailbox)` (`.Seq_Cst` load of `mailbox.head`).
Because `.Seq_Cst` operations form a single total order, either the producer observes `a.scheduled == 0` and reschedules the actor, or the worker's post-unschedule probe observes `head != tail` and reschedules the actor itself.

### 4.5. Stack-Local `ctx.forwarded` vs. Shared Envelope Flags
When an actor calls `actor.forward(ctx, next_actor, updated_msg)`, the active envelope is pushed directly into `next_actor.mailbox`.
* If `forward` mutated `env.flags` to tell the caller worker not to recycle `env`, two bugs would occur: (1) a data race between the caller worker reading `env.flags` after `receive()` and the target worker popping `env` on another core, and (2) `env.flags` remaining marked as forwarded when the final sink actor finishes processing it, leaking the pool slot.
* Storing `forwarded: bool` on the worker's stack-local `Context` (`ctx.forwarded = true`) avoids touching `env` after `send_envelope` and ensures that when the final actor in a pipeline consumes the message without calling `forward`, `ctx.forwarded` is `false` and the envelope is cleanly returned to its origin `Envelope_Pool`.

### 4.6. Cache-Line Padding Rules (`#align(64)`)
* **DO align mailbox and ring pointers (`MPSC_Queue`, `SPSC_Lane`, `Actor_Base`):** Placing `head` (producer-written) and `tail` (consumer-written) on separate 64-byte cache lines prevents false sharing between producers and consumers.
* **DO NOT align individual `Envelope($Msg)` structs to 64 bytes:** A compact `Envelope(i64)` is only **24 bytes** (`next: rawptr`, `pool_id: u16`, `flags: u16`, `msg: i64`—matching `libponyrt`'s 24-byte `pony_msgi_t`), allowing 2.66 envelopes per 64-byte cache line. Padding every message node to 64 bytes cuts L2/L3 cache capacity by 2.6x and more than halves throughput during batch drains.
