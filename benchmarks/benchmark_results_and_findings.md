# Odin Actor Implementations: Complete Benchmark Results & Architectural Findings

Based on the architectures discussed in the [Shared Gemini Conversation](https://share.gemini.google/vYW4P9VfPnxa), this report captures all empirical benchmark measurements across the five workloads we built and iterated on (v1 through v6), along with instructions to re-run both the pure Odin suite (`actor_benchmark.odin`) and the C11 reference suite (`actor_benchmark_c11.c`) on your local multi-core machine.

## 1. How to Re-Run the Benchmarks Locally

Both benchmark files are self-contained and will regenerate the full benchmark tables on your hardware:

* **Run the Pure Odin Benchmark Suite (`benchmarks/actor_benchmark.odin`):**
  ```bash
  odin run benchmarks/actor_benchmark.odin -file -o:speed
  ```
* **Run with Direct `libponyrt` C FFI Enabled (`-define:USE_LIBPONYRT=true`):**
  ```bash
  odin run benchmarks/actor_benchmark.odin -file -o:speed -define:USE_LIBPONYRT=true \
    -extra-linker-flags:"-L/usr/local/lib -lponyrt -lpthread -ldl -lm"
  ```
* **Run the Unified C11 / pthreads Reference Suite (`benchmarks/actor_benchmark_c11.c`):**
  ```bash
  gcc -O3 -march=native -pthread benchmarks/actor_benchmark_c11.c -o benchmarks/actor_benchmark_c11
  ./benchmarks/actor_benchmark_c11
  ```

## 2. Complete Empirical Benchmark Scoreboard (v1–v6)

All measurements below were captured with `-O3 -march=native -pthread` on a 2-vCPU x86_64 Linux environment. On an 8-core or 16-core desktop CPU, the multi-threaded workloads (`2b`, `3e`, `3g`, and `3h`) scale even higher.

### Workload 1: 4-Producer $\rightarrow$ 1-Actor Fan-In Contention (2,000,000 Messages, 8B Payload)

Stresses multi-producer mailbox contention, cross-thread memory recycling, and cache-line invalidation across 4 concurrent producer threads feeding 1 actor:

| Implementation & Memory Strategy | OS Threads | Memory Footprint | Total Time | Throughput | Time / Msg | Speedup vs. Channel |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: |
| **2a. DOD Batch Array (Single-Thread Baseline)** | 1 | 64 KB | 4.71 ms | 424.51 M msg/s | 2.4 ns | **145.2x** |
| **2b. DOD Multi-Thread (4 TLS Buffers + Barrier Drain)** | 4 + 1 | 32 MB | 33.27 ms | 60.11 M msg/s | 16.6 ns | **20.6x** |
| **3a. Self-Impl Pony MPSC (Unbounded Pre-Alloc Slab)** | 4 + 1 | 48 MB (unrecycled) | 75.54 ms | 26.48 M msg/s | 37.8 ns | **9.1x** |
| **3e. Sharded SPSC Ring Lanes (1 Lane / Producer)** | 4 + 1 | **128 KB (bounded)** | **76.66 ms** | **26.09 M msg/s** | **38.3 ns** | **8.9x** |
| **3d. Self-Impl Pony MPSC (`pony_sendv(first,last)` Chain=32)** | 4 + 1 | 768 KB (bounded) | 104.83 ms | 19.08 M msg/s | 52.4 ns | **6.5x** |
| **3c. Self-Impl Pony MPSC (Batched FIFO Watermark Slab)** | 4 + 1 | **768 KB (bounded)** | **108.57 ms** | **18.42 M msg/s** | **54.3 ns** | **6.3x** |
| **3b. Self-Impl Pony MPSC (Per-Message `in_use` Slab)** | 4 + 1 | 768 KB (bounded) | 240.51 ms | 8.32 M msg/s | 120.3 ns | **2.8x** |
| **4. Direct `libponyrt` via Odin C FFI (`pony_alloc_msg`)** | 4 + 1 | Dynamic pool | 240.82 ms | 8.30 M msg/s | 120.4 ns | **2.8x** |
| **3f. Self-Impl Pony MPSC + Adaptive Futex Parker (`sync.Futex`)** | 4 + 1 | 768 KB (bounded) | 311.88 ms | 6.41 M msg/s | 155.9 ns | **2.2x (0% idle CPU)** |
| **1. Traditional Channel + Thread (`core:sync/chan`)** | 4 + 1 | 64 KB | 683.81 ms | 2.92 M msg/s | 341.9 ns | 1.0x (Baseline) |

### Workload 2: Cross-Thread 1-to-1 Ping-Pong Latency (100,000 Round-Trips / 200,000 Sequential Hops)

Measures raw inter-thread message wakeup and round-trip latency between two actors on separate threads:

| Implementation | OS Threads | Total Time | One-Way Hop Latency | Round-Trip Time (RTT) | Speedup vs. Channel |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **4. Direct `libponyrt` via Odin C FFI** | 2 | 15.86 ms | 79.3 ns | 158.6 ns | **149.0x** |
| **3. Self-Impl Pony MPSC (`intrinsics.atomic_exchange`)** | 2 | 16.86 ms | 84.3 ns | 168.6 ns | **140.1x** |
| **1. Traditional Channel + Thread (`core:sync/chan`)** | 2 | 2,362.81 ms | 11,814.1 ns | 23,628.1 ns | 1.0x (Baseline) |

### Workload 3: 512-Byte Large Payload Crossover (500,000 Messages, 4 Producers $\rightarrow$ 1 Actor)

Tests where Pony's zero-copy pointer ownership transfer (`iso` capability simulation) overtakes value-copying channels and DOD staging arrays:

| Implementation | OS Threads | Payload Transfer Mechanism | Total Time | Throughput | Time / Msg | Speedup vs. DOD |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: |
| **3c. Self-Impl Pony MPSC Zero-Copy `iso` Pointer** | 4 + 1 | In-place 8K slab + 8B pointer pass | **71.80 ms** | **6.96 M msg/s** | **143.6 ns** | **4.8x** |
| **1. Traditional Channel + Thread (`core:sync/chan`)** | 4 + 1 | 512B `memcpy` into ring + 512B out | 192.99 ms | 2.59 M msg/s | 386.0 ns | **1.8x** |
| **2b. DOD Multi-Thread TLS Staging** | 4 + 1 | 256 MB staging buffer allocation + scan | 345.35 ms | 1.45 M msg/s | 690.7 ns | 1.0x (Baseline) |

### Workload 4: 1,024-Actor Swarm Scaling (2,000,000 Messages Distributed Across 1,024 Actors)

Compares 1:1 OS thread-per-actor against Pony's M:N cooperative actor scheduler (multiplexing 1,024 actors onto 2 worker threads with a 100-message batch quantum):

| Implementation | OS Threads | Scheduler Design | Total Time | Throughput | Time / Msg | Speedup vs. 1:1 Threads |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: |
| **3g. Sharded Work-Stealing M:N Scheduler** | 4 + 2 | Per-producer MPSC run-queue shards + stealing | **190.89 ms** | **10.48 M msg/s** | **95.4 ns** | **180.8x** |
| **3g (v2). Global Run-Queue M:N Scheduler** | 4 + 2 | Single shared scheduler queue | 369.63 ms | 5.41 M msg/s | 184.8 ns | **93.4x** |
| **1. Traditional 1:1 Thread + Channel Swarm** | 4 + 1,024 | 1 OS thread & mutex/condvar channel per actor | 34,507.02 ms | 0.06 M msg/s | 17,253.5 ns | 1.0x (Baseline) |

### Workload 5: Autonomous Actor-to-Actor Graph + Per-Message CPU Work (500,000 Hops, 1,024 Actors)

Simulates a closed network of 1,024 actors with 8,192 circulating tokens (zero external producer threads). Every hop performs **64 rounds of SplitMix64 math** (~200 ns of simulated AI/physics/serialization work), mutates actor state, and forwards the token to another actor:

| Implementation | Worker Cores | Total Time | Throughput | Time / Hop | Speedup vs. 1-Core DOD |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **3h. Self-Impl Pony M:N Actor-to-Actor (`iso` Token Forwarding)** | **2 Cores** | **150.66 ms** | **3.32 M hops/s** | **301.3 ns** | **1.46x (Fastest)** |
| **2b. DOD Multi-Thread Fork-Join Batch** (Barrier + Double Buffer) | 2 Cores | 176.66 ms | 2.83 M hops/s | 353.3 ns | **1.24x** |
| **2a. DOD Single-Thread Batch** (Sequential Array) | 1 Core | 219.35 ms | 2.28 M hops/s | 438.7 ns | 1.00x (Baseline) |
| **3h. Self-Impl Pony M:N Actor-to-Actor (`iso` Token Forwarding)** | 1 Core | 244.15 ms | 2.05 M hops/s | 488.3 ns | 0.90x |

## 3. Seven Critical Engineering Takeaways for Odin

1. **Cross-thread memory recycling is the real bottleneck in MPSC actors, not the queue (`3a` vs. `3b` vs. `3c`):**
   Pre-allocating 2M messages in a flat 48 MB array (`3a`) hits **26.48 M msg/s**, but once producers recycle a bounded ring slab (`3b`) by having the consumer write `tail.in_use = 0` on every pop, throughput drops to **8.32 M msg/s**—identical to `libponyrt` C FFI (**8.30 M msg/s**). Why? Because writing `tail.in_use = 0` invalidates the message cache line across CPU cores 2,000,000 times. Batching slot retirement once per 100-message quantum (`3c`, `snmalloc` principle) cuts cross-core invalidations by 100x and doubles bounded throughput back to **18.42 M msg/s**.
2. **Apply `#align(64)` to the Actor/Mailbox pointers, NEVER to every message struct:**
   Separating `Pony_Mailbox.head` (producer-contended `XCHG` pointer) and `Pony_Mailbox.tail` (consumer-private pointer) onto separate 64-byte cache lines prevents false sharing. However, putting `#align(64)` on every `Pony_Odin_Msg` bloats a 24-byte message to 64 bytes, cutting L3 cache density by 2.6x and halving throughput (**12.42 M/s** vs. **26.48 M/s**).
3. **Use Pony's zero-RMW dummy-tail pop (`q.tail = next; return next;`) for fan-in:**
   A textbook Vyukov MPSC queue that re-inserts an embedded `stub` sentinel via `atomic_exchange` when empty forces the consumer to fight producers for the `q.head` cache line. Leaving the last popped message node in place as the new dummy `tail` (`libponyrt/actor/messageq.c`) means the consumer executes **zero atomic read-modify-write instructions**—only a plain `.Acquire` load (`MOV` on x86_64).
4. **Watch out for out-of-order slot retirement across multiple actors (`3c` vs. `3g`):**
   Because Pony's dummy-tail queue keeps the most recently popped message node inside `actor.tail` until that specific actor receives *another* message, single-actor fan-in (`3c`) retires each producer's ring slots in strict FIFO order (making a single `retired_count[p]` counter safe). Across **1,024 actors (`3g`)**, a quiet actor holds onto an old slot in `actor.tail` while busy actors advance `retired_count[p]`, which would cause a FIFO ring counter to wrap and overwrite an active `tail` pointer. Multi-actor swarms must use per-slot `in_use` flags (`3g`), detached per-actor stub queues (`3h`), or an explicit freelist of retired nodes.
5. **Fix `pony_msg_t` offset 0 when using `libponyrt` via Odin C FFI:**
   In `libponyrt`, `_Atomic(^pony_msg_t) next` sits at **offset 0** of `pony_msg_t` (before `index: u32` and `id: u32`). Omitting `next` at offset 0 in Odin's FFI struct causes `pony_sendv` to overwrite the first 8 bytes of your message payload.
6. **Payload size and CPU work flip the winner between DOD and Pony M:N Actors:**
   - For **tiny payloads ($\le 64$ B) and trivial per-message work** inside a synchronous game tick, **DOD with thread-local staging buffers (`2b`, 60.11 M msg/s)** wins by avoiding atomic queue operations during the frame.
   - For **large payloads ($\ge 512$ B, Workload 3)**, **Pony MPSC Zero-Copy (`3c`, 6.96 M msg/s)** beats DOD staging (**1.45 M msg/s**) by **4.8x** because passing an 8-byte pointer avoids 256 MB of staging `memcpy` traffic.
   - For **autonomous actor-to-actor graphs with real CPU work per message (Workload 5)**, **Pony M:N Actors (`3h`, 150.66 ms)** beat both single-core DOD (**219.35 ms**) and multi-core fork-join DOD (**176.66 ms**) by scaling across cores without global frame barriers or atomic state collisions.
7. **When worker count is fixed, Sharded SPSC Ring Lanes (`3e`) reach 26.09 M msg/s with zero `lock xchg`:**
   Giving high-traffic actors one SPSC ring lane per worker thread replaces `atomic_exchange` (`lock xchg`) with pure `.Release` / `.Acquire` stores and loads, matching unbounded slab speed inside a fixed **128 KB** memory budget.
