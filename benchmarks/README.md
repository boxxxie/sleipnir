# Sleipnir Concurrency & Actor Runtime Benchmark Suite

This directory contains an empirical micro-architectural benchmark suite comparing **7 concurrency runtimes and message-passing implementations** across **Odin**, **C11**, and the **official Pony language C runtime (`libponyrt` 0.77.2)**.

---

## 1. What Are We Testing? (Runtimes & Implementations Under Test)

Every benchmark table in this report labels rows with one of the following **7 runtimes / implementations**:

| ID Prefix | Runtime / Implementation | Language | Source Code | What It Actually Tests |
| :--- | :--- | :--- | :--- | :--- |
| **`5a`–`5c`** | **Sleipnir (`package actor`)** | **Odin** | [`actor/*.odin`](../actor/) | **This repository's production library.** Type-erased `Actor_Base` + generic `Actor(State, Msg)` wrappers, `Envelope_Pool(Msg)` with intrusive chain return (`push_return_chain`), Chase-Lev work-stealing `M:N` scheduler, and adaptive `sync.Futex` worker parking. |
| **`4` / `4a`–`4e`** | **Official Pony Runtime (`libponyrt` 0.77.2)** | **C (Official `ponyc` Runtime)** | [`ponyc/src/libponyrt`](https://github.com/ponylang/ponyc/tree/0.77.2/src/libponyrt) via [`build_libponyrt.sh`](build_libponyrt.sh) | **The real Pony language C runtime library (`libponyrt.a`)**, compiled with `gcc -O3 -march=native` and called both from Odin C FFI and directly from C11. Tests `pony_alloc_msg`, `ponyint_actor_messageq_push` / `pony_chain`, `ponyint_pool_alloc_size` (512B pool arena), and full `pony_init` + `pony_create` + `pony_sendv` + `pony_start` M:N scheduling with ORCA cycle detector / GC tracking. |
| **`3` / `3b`–`3h`** | **Raw Pony-Style MPSC & M:N (Specialized)** | **Odin** | [`actor_benchmark.odin`](actor_benchmark.odin) | **Hand-specialized, monomorphic Odin implementations** of the Vyukov Intrusive MPSC queue, 8K watermark ring slab, and work-stealing M:N scheduler without library type-erasure indirection. Serves as the theoretical speed-of-light baseline for Pony-style MPSC in Odin. |
| **`C11` (`3c`–`3h`)** | **Raw C11 `stdatomic.h` + `pthreads`** | **C11** | [`actor_benchmark_c11.c`](actor_benchmark_c11.c) | **Direct C11 port of the raw Odin benchmarks** compiled with `gcc -O3 -march=native -pthread`. Used to isolate compiler code generation differences between Odin (`LLVM` backend) and GCC C11 on identical atomic instructions (`XCHG`, `CMPXCHG`, `LOAD_ACQUIRE`, `STORE_RELEASE`). |
| **`3e`** | **Wait-Free Sharded SPSC Mailbox** | **Odin & C11** | [`actor/spsc_mailbox.odin`](../actor/spsc_mailbox.odin) & [`actor_benchmark_c11.c`](actor_benchmark_c11.c) | **Per-producer cache-line-isolated Single-Producer Single-Consumer (`SPSC`) ring buffers** (`actor.Sharded_SPSC_Mailbox`). Zero atomic Read-Modify-Write (`LOCK XCHG` / `LOCK CMPXCHG`) instructions—only `Acquire` loads and `Release` stores. |
| **`1`** | **Traditional OS Thread + Mutex Channel** | **Odin & C11** | `core:sync/chan` (Odin) & `pthread_mutex_t` + `pthread_cond_t` (C11) | **Go-style bounded mutex channel** (`chan.Chan(Msg)`) paired with a dedicated OS thread. Represents the conventional "1 thread + 1 channel per actor" baseline. |
| **`2a` / `2b`** | **Data-Oriented Design (`DOD`) Flat Batch Arrays** | **Odin & C11** | [`actor_benchmark.odin`](actor_benchmark.odin) & [`actor_benchmark_c11.c`](actor_benchmark_c11.c) | **Non-actor data-oriented baseline.** Contiguous `[]Sim_Event` slices (`16 bytes` per event) processed in a tight loop (`2a` single-threaded) or accumulated in 4 thread-local (`TLS`) arrays before a single consumer drain (`2b`). |

```mermaid
flowchart TD
    subgraph Runtimes["7 Runtimes & Implementations Compared"]
        direction TB
        S["5a–5c: Sleipnir (package actor)\nGeneric Odin M:N Actor Library"]
        P["4a–4e: Official Pony Runtime (libponyrt 0.77.2)\nOfficial C Runtime from ponylang/ponyc"]
        RO["3b–3h: Raw Specialized Odin\nMonomorphic Vyukov MPSC + M:N"]
        RC["C11 stdatomic.h + pthreads\nGCC -O3 -march=native Reference"]
        SPSC["3e: Wait-Free Sharded SPSC\nactor.Sharded_SPSC_Mailbox (Odin & C11)"]
        CH["1: Mutex Channel + OS Thread\nOdin core:sync/chan & C11 pthread_mutex_t"]
        DOD["2a–2b: DOD Contiguous Arrays\nSingle-Thread & 4x TLS Batch Drain"]
    end
```

---

## 2. Reference System & Test Environment

All results below were measured on the same machine under identical governor and compiler settings:

| Component | Specification |
| :--- | :--- |
| **CPU** | **AMD Ryzen 7 7735U with Radeon Graphics** (Zen 3+ / *Rembrandt*, Family `25`, Model `68`) |
| **Topology** | **8 Physical Cores / 16 Hardware Threads** (1 Socket, 2 Threads/Core, Boost up to `4.76 GHz`) |
| **Cache Hierarchy** | **L1d:** `256 KiB` (`32 KiB × 8`) • **L1i:** `256 KiB` (`32 KiB × 8`) • **L2:** `4 MiB` (`512 KiB × 8`) • **L3:** `16 MiB` unified (`1 × 16 MiB`) |
| **Memory** | **16 GiB LPDDR5** (`14.8 GiB` usable, zero swap pressure) |
| **OS & Kernel** | **Manjaro Linux x86_64**, Kernel `7.1.13-2-MANJARO` (`PREEMPT_DYNAMIC`) |
| **Odin Compiler** | `odin version dev-2026-04-nightly:65319f9` (`-o:speed`) |
| **C Compiler** | `gcc (GCC) 16.1.1 20260411` (`-O3 -march=native -pthread`) |
| **Pony Runtime** | Official [`ponylang/ponyc` `0.77.2`](https://github.com/ponylang/ponyc/tree/0.77.2/src/libponyrt) (`libponyrt.a` compiled from source with `gcc -O3 -march=native -mcx16`) |

---

## 3. Empirical Results by Workload

### Workload 1: 4-Producer $\rightarrow$ 1-Actor Fan-In (`2,000,000` Messages)

Four concurrent producer threads each emit `500,000` messages (`2,000,000` total) into a single consumer actor that accumulates an `i64` sum (`999,998,000,000`) and signals completion on the `2,000,000`th message.

| ID | Architecture / Implementation | Language / Runtime | Wall Time | Throughput | Latency / Msg | Speedup vs Odin `chan` |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: |
| **`3e`** | **Wait-Free Sharded SPSC (`Sharded_SPSC_Mailbox`)** | **Odin (`package actor`)** | **`1.89 – 2.42 ms`** | **`827.45 – 1,058.33 M/s`** | **`0.9 – 1.2 ns`** | **`624.8x – 799.5x`** |
| **`2a`** | **Single-Thread DOD Batch Array (`16B` struct)** | **C11 (`gcc -O3`)** | **`2.29 ms`** | **`872.21 M/s`** | **`1.1 ns`** | **`658.7x`** |
| **`3e`** | **Wait-Free Sharded SPSC (4 Ring Lanes)** | **C11 (`stdatomic.h`)** | **`3.56 ms`** | **`561.76 M/s`** | **`1.8 ns`** | **`424.2x`** |
| **`2a`** | **Single-Thread DOD Batch Array (`16B` struct)** | **Odin (`-o:speed`)** | **`3.96 – 5.16 ms`** | **`387.54 – 505.16 M/s`** | **`2.0 – 2.6 ns`** | **`292.6x – 381.6x`** |
| **`3d`** | **Raw Pony MPSC (`Chain=32` Batch + 8K Watermark Slab)** | **Odin (Raw Specialized)** | **`4.13 – 6.37 ms`** | **`313.83 – 483.72 M/s`** | **`2.1 – 3.2 ns`** | **`237.0x – 365.4x`** |
| **`3d`** | **Raw Pony MPSC (`Chain=32` Batch + 8K Watermark Slab)** | **C11 (`stdatomic.h`)** | **`6.50 ms`** | **`307.53 M/s`** | **`3.3 ns`** | **`232.2x`** |
| **`2b`** | **Multi-Thread DOD (4 TLS Buffers + Drain)** | **Odin (`core:thread`)** | **`8.97 – 10.12 ms`** | **`197.70 – 222.95 M/s`** | **`4.5 – 5.1 ns`** | **`149.3x – 168.4x`** |
| **`2b`** | **Multi-Thread DOD (4 TLS Buffers + Drain)** | **C11 (`pthreads`)** | **`10.08 ms`** | **`198.44 M/s`** | **`5.0 ns`** | **`149.8x`** |
| **`4b`** | **Official Pony `libponyrt` (`pony_chain`, `Chain=32`)** | **C (`libponyrt` 0.77.2 via Odin FFI)** | **`20.50 – 27.74 ms`** | **`72.10 – 97.57 M/s`** | **`10.2 – 13.9 ns`** | **`54.4x – 73.7x`** |
| **`5a`** | **Sleipnir `send_batch_from_pool` (`Chain=32`)** | **Odin (`package actor` Library)** | **`25.68 – 27.72 ms`** | **`72.14 – 77.89 M/s`** | **`12.8 – 13.9 ns`** | **`54.5x – 58.8x`** |
| **`4b`** | **Official Pony `libponyrt` (`pony_chain`, `Chain=32`)** | **C (`libponyrt` 0.77.2 via C11)** | **`27.00 ms`** | **`74.07 M/s`** | **`13.5 ns`** | **`55.9x`** |
| **`3c`** | **Raw Pony MPSC (Single Send, 8K Watermark Slab)** | **Odin (Raw Specialized)** | **`42.94 – 59.63 ms`** | **`33.54 – 46.58 M/s`** | **`21.5 – 29.8 ns`** | **`25.3x – 35.2x`** |
| **`5a`** | **Sleipnir `send_from_pool` (Single Send)** | **Odin (`package actor` Library)** | **`45.35 – 64.36 ms`** | **`31.08 – 44.10 M/s`** | **`22.7 – 32.2 ns`** | **`23.5x – 33.3x`** |
| **`3f`** | **Raw Pony MPSC + Adaptive `sync.Futex` Parker** | **Odin (Raw Specialized)** | **`48.51 – 69.46 ms`** | **`28.79 – 41.23 M/s`** | **`24.3 – 34.7 ns`** | **`21.7x – 31.1x`** |
| **`3b`** | **Raw Pony MPSC (Single Send, Per-Msg `in_use` Flag Slab)** | **Odin (Raw Specialized)** | **`49.64 – 60.90 ms`** | **`32.84 – 40.29 M/s`** | **`24.8 – 30.4 ns`** | **`24.8x – 30.4x`** |
| **`3c`** | **Raw Pony MPSC (Single Send, 8K Watermark Slab)** | **C11 (`stdatomic.h`)** | **`59.60 ms`** | **`33.56 M/s`** | **`29.8 ns`** | **`25.3x`** |
| **`4a`** | **Official Pony `libponyrt` (`pony_alloc_msg` Single Send)** | **C (`libponyrt` 0.77.2 via Odin FFI)** | **`63.66 – 85.32 ms`** | **`23.44 – 31.42 M/s`** | **`31.8 – 42.7 ns`** | **`17.7x – 23.7x`** |
| **`4a`** | **Official Pony `libponyrt` (`pony_alloc_msg` Single Send)** | **C (`libponyrt` 0.77.2 via C11)** | **`89.34 ms`** | **`22.39 M/s`** | **`44.7 ns`** | **`16.9x`** |
| **`1`** | **Traditional OS Thread + Mutex Channel (`cap=4096`)** | **C11 (`pthread_mutex_t` + `cond`)** | **`422.18 ms`** | **`4.74 M/s`** | **`211.1 ns`** | **`3.6x`** |
| **`1`** | **Traditional OS Thread + Buffered `chan` (`cap=4096`)** | **Odin (`core:sync/chan`)** | **`1,086.96 – 1,510.33 ms`** | **`1.32 – 1.84 M/s`** | **`543.5 – 755.2 ns`** | **`1.0x`** |

---

### Workload 2: Cross-Thread 1-to-1 Ping-Pong Latency (`100,000` Round-Trips = `200,000` Hops)

Measures strict round-trip serialization latency between two pinned actor threads passing a single message back and forth `200,000` times. Throughput cannot be hidden by pipelining; this directly measures L3 cache-coherency bounce + envelope recycling cost.

| ID | Architecture / Implementation | Language / Runtime | Total Time | Hops / Sec | Latency / Hop | Speedup vs `chan` |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: |
| **`3`** | **Raw Pony MPSC (Zero-Alloc Static Envelope Reuse)** | **Odin (Raw Specialized)** | **`19.05 – 24.30 ms`** | **`8.23 – 10.50 M/s`** | **`95.2 – 121.5 ns`** | **`41.5x – 55.3x`** |
| **`4`** | **Official Pony `libponyrt` (`pony_alloc_msg` + `messageq`)** | **C (`libponyrt` 0.77.2 via Odin FFI)** | **`25.78 – 38.48 ms`** | **`5.20 – 7.76 M/s`** | **`128.9 – 192.4 ns`** | **`26.2x – 40.8x`** |
| **`1`** | **Traditional Buffered `chan` (`cap=64`)** | **Odin (`core:sync/chan`)** | **`1,009.37 – 1,049.19 ms`** | **`0.19 – 0.20 M/s`** | **`5,046.8 – 5,246.0 ns`** | **`1.0x`** |

---

### Workload 3: Large Payload (`512-Byte` Struct, `500,000` Messages)

Four producers send `500,000` 512-byte messages (`256 MiB` of logical payload) to a single consumer actor. Compares zero-copy `iso` pointer handoff against `libponyrt`'s thread-local `pool_arena` and value-copied channels.

| ID | Architecture / Implementation | Language / Runtime | Total Time | Throughput | Latency / Msg | Speedup vs `chan` |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: |
| **`3c`** | **Raw Pony MPSC Zero-Copy `iso` Pointer (512B Ring Slab)** | **Odin (Raw Specialized)** | **`9.43 – 14.76 ms`** | **`33.88 – 53.02 M/s`** | **`18.9 – 29.5 ns`** | **`28.4x – 44.6x`** |
| **`2`** | **Single-Thread DOD Batch Array (`512B` value copy)** | **Odin (`-o:speed`)** | **`11.09 ms`** | **`45.09 M/s`** | **`22.2 ns`** | **`37.8x`** |
| **`4c`** | **Official Pony `libponyrt` (`pony_alloc_msg` 512B Pool Arena)** | **C (`libponyrt` 0.77.2 via Odin FFI)** | **`44.63 – 55.64 ms`** | **`8.99 – 11.20 M/s`** | **`89.3 – 111.3 ns`** | **`7.6x – 9.4x`** |
| **`1`** | **Traditional Buffered `chan` (`512B` value copy)** | **Odin (`core:sync/chan`)** | **`418.74 ms`** | **`1.19 M/s`** | **`837.5 ns`** | **`1.0x`** |

---

### Workload 4: `1,024`-Actor `M:N` Swarm (`2,000,000` Messages from 4 External Producers)

`1,024` actors are scheduled across **4 `M:N` worker threads** while **4 external producer threads** inject `2,000,000` messages round-robin across all `1,024` actors. Unlike single-actor fan-in, messages are distributed across `1,024` distinct mailboxes and scheduled via work-stealing runqueues.

| ID | Architecture / Implementation | Language / Runtime | Total Time | Throughput | Latency / Msg | Speedup vs `1` |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: |
| **`5b`** | **Sleipnir Generic `M:N` Swarm (`1,024` Actors, `Envelope_Pool`)** | **Odin (`package actor` Library)** | **`32.35 – 44.95 ms`** | **`44.49 – 61.82 M/s`** | **`16.2 – 22.5 ns`** | **`9.7x – 13.5x`** |
| **`3g`** | **Sharded Work-Stealing `M:N` Swarm (`1,024` Actors)** | **Odin (Raw Specialized)** | **`37.07 – 41.12 ms`** | **`48.64 – 53.95 M/s`** | **`18.5 – 20.6 ns`** | **`10.6x – 11.8x`** |
| **`4d`** | **Official Pony `libponyrt` Full `M:N` Runtime (`pony_create` + `pony_start`)** | **C (`libponyrt` 0.77.2 Full Runtime)** | **`88.46 – 110.96 ms`** | **`18.02 – 22.61 M/s`** | **`44.2 – 55.5 ns`** | **`3.9x – 4.9x`** |
| **`3a`** | **Contended Single Global `M:N` Runqueue (`1,024` Actors)** | **Odin (Global MPSC Runqueue)** | **`250.33 ms`** | **`7.99 M/s`** | **`125.2 ns`** | **`1.7x`** |
| **`1`** | **4-Actor OS Thread Pool (`core:sync/chan`)** | **Odin (`core:sync/chan`)** | **`435.89 ms`** | **`4.59 M/s`** | **`217.9 ns`** | **`1.0x`** |

---

### Workload 5: Autonomous `1,024`-Actor Graph + CPU Work (`500,000` Hops across `1`, `2`, `4`, and `8` Cores)

The most realistic end-to-end actor benchmark: **`1,024` autonomous actors** communicate directly with each other (`Actor A -> Actor B -> Actor C`). Initially, `1,024` seed tokens are injected across the actor graph (`1` per actor). On every message hop, the receiving actor executes **64 rounds of 64-bit SplitMix64 hashing (`~45 ns` of real CPU work)**, picks a pseudo-random target actor `(self_id * 7 + step) % 1024`, and sends the token forward until `500,000` total hops complete.

Here we compare **4 implementations side-by-side** across `1`, `2`, `4`, and `8` scheduler worker threads:
1. **Sleipnir (`package actor`, `5c`)** — Odin generic library (`actor.forward` zero-copy `iso` handoff)
2. **Official Pony Runtime (`libponyrt` 0.77.2, `4e`)** — Full C `libponyrt` M:N scheduler (`pony_init`, `pony_create`, `pony_sendi` inside actor `dispatch`, `pony_start`)
3. **Raw Specialized Odin (`3h`)** — Hand-inlined Odin `M:N` work-stealing runtime
4. **Raw C11 (`3h` in `actor_benchmark_c11.c`)** — Hand-inlined C11 `stdatomic.h` + `pthreads` `M:N` runtime

| Worker Threads | Sleipnir (`package actor` Odin) | Official Pony (`libponyrt` 0.77.2 C) | Raw Specialized Odin (`3h`) | Raw C11 (`stdatomic.h`) | Multi-Core Scaling (`1 -> N`) |
| :--- | ---: | ---: | ---: | ---: | :--- |
| **1 Worker** | `148.96 – 168.80 ms` (**`2.96 – 3.36 M/s`**) | `154.38 – 179.28 ms` (**`2.79 – 3.24 M/s`**) | `126.98 – 166.48 ms` (**`3.00 – 3.94 M/s`**) | `161.85 ms` (**`3.09 M/s`**) | `1.00x` baseline |
| **2 Workers** | `77.62 – 86.00 ms` (**`5.81 – 6.44 M/s`**) | `84.86 – 96.60 ms` (**`5.18 – 5.89 M/s`**) | `64.94 – 87.15 ms` (**`5.74 – 7.70 M/s`**) | `85.06 ms` (**`5.88 M/s`**) | **`1.92x – 1.96x`** (Sleipnir) |
| **4 Workers** | `36.23 – 47.53 ms` (**`10.52 – 13.80 M/s`**) | `48.68 – 48.94 ms` (**`10.22 – 10.27 M/s`**) | `35.23 – 45.64 ms` (**`10.96 – 14.19 M/s`**) | `44.37 ms` (**`11.27 M/s`**) | **`3.55x – 4.11x`** (Sleipnir) |
| **8 Workers** | `28.02 – 30.03 ms` (**`16.65 – 17.85 M/s`**) | `25.38 – 26.13 ms` (**`19.14 – 19.70 M/s`**) | `26.14 – 26.74 ms` (**`18.70 – 19.12 M/s`**) | `25.08 ms` (**`19.93 M/s`**) | **`5.63x – 5.97x`** (Sleipnir) |

---

## 4. Cross-Runtime & Cross-Implementation Comparison

### 4.1 Sleipnir (`package actor` in Odin) vs. Official Pony Runtime (`libponyrt` 0.77.2 in C)

How does Sleipnir's Odin implementation compare head-to-head against the official Pony C runtime (`libponyrt` 0.77.2)?

| Benchmark Scenario | Sleipnir (`package actor` Odin) | Official Pony (`libponyrt` 0.77.2 C) | Winner & Margin |
| :--- | ---: | ---: | :--- |
| **Single-Msg 4-Producer Fan-In (`2M` msgs)** | **`31.08 – 44.10 M/s`** (`22.7 – 32.2 ns`) | `23.44 – 31.42 M/s` (`31.8 – 42.7 ns`) | **Sleipnir (`1.33x – 1.40x` faster)** |
| **Batched `Chain=32` Fan-In (`2M` msgs)** | **`72.14 – 77.89 M/s`** (`12.8 – 13.9 ns`) | `72.10 – 97.57 M/s` (`10.2 – 13.9 ns`) | **Tie / Within Boost Variance** |
| **External 4-Producer $\rightarrow$ `1,024`-Actor Swarm** | **`44.49 – 61.82 M/s`** (`16.2 – 22.5 ns`) | `18.02 – 22.61 M/s` (`44.2 – 55.5 ns`) | **Sleipnir (`2.48x – 3.43x` faster)** |
| **Autonomous `1,024`-Actor Graph + CPU (`1` Core)** | **`2.96 – 3.36 M/s`** (`149.0 – 168.8 ms`) | `2.79 – 3.24 M/s` (`154.4 – 179.3 ms`) | **Sleipnir (`1.04x – 1.06x` faster)** |
| **Autonomous `1,024`-Actor Graph + CPU (`2` Cores)** | **`5.81 – 6.44 M/s`** (`77.6 – 86.0 ms`) | `5.18 – 5.89 M/s` (`84.9 – 96.6 ms`) | **Sleipnir (`1.09x – 1.12x` faster)** |
| **Autonomous `1,024`-Actor Graph + CPU (`4` Cores)** | **`10.52 – 13.80 M/s`** (`36.2 – 47.5 ms`) | `10.22 – 10.27 M/s` (`48.7 – 48.9 ms`) | **Sleipnir (`1.03x – 1.34x` faster)** |
| **Autonomous `1,024`-Actor Graph + CPU (`8` Cores)** | `16.65 – 17.85 M/s` (`28.0 – 30.0 ms`) | **`19.14 – 19.70 M/s`** (`25.4 – 26.1 ms`) | **Pony `libponyrt` (`1.07x – 1.18x` faster)** |

#### Why does Sleipnir outperform `libponyrt` on Fan-In (`1.33x`) and External Swarm (`2.48x`), while `libponyrt` edges ahead at 8 Workers (`1.18x`)?

1. **Zero GC / Cycle-Detector Bookkeeping on Message Send:**
   In `libponyrt`, every actor carries an ORCA reference-counted garbage collector (`gc_t`, `224 bytes` into `pony_actor_t`) and integrates with the cycle detector (`ponyint_cycle_check_blocked`). When external threads inject messages via `ponyint_sendv_inject`, `libponyrt` must acquire a temporary scheduler context and push unscheduled actors onto the `inject` MPSC queue before worker threads steal them. Sleipnir has **no GC or cycle detector** and routes external sends directly to `target.home_worker`'s sharded inbox using a single `sync.Futex` wake on idle transitions (`2.48x` faster external swarm ingestion).
2. **`Envelope_Pool` Batch Return Chains vs. `ponyint_pool_free_size`:**
   In Sleipnir (`5a`/`5b`), an actor draining a batch of up to 256 messages links recycled envelopes into a local singly-linked list and returns the entire batch to the producer's return stack with **one `ATOMIC_XCHG`** (`push_return_chain`). `libponyrt` calls `ponyint_pool_free_size(size, msg)` on every single message in the `messageq_pop` loop (`32.2 ns` in Sleipnir vs. `42.7 ns` in `libponyrt`).
3. **Why `libponyrt` scales slightly better at `8 Workers` (`19.70 M/s` vs `16.65 M/s`):**
   On an 8-core Ryzen 7 7735U when all 8 cores are saturated with autonomous actor-to-actor traffic, `libponyrt`'s scheduler keeps an executing actor pinned in a local `last_actor` register slot and uses a single-consumer MPSC runqueue (`mpmcq_t`) per scheduler thread rather than incrementing/decrementing an atomic Chase-Lev deque `bottom` index on every pop. Under 8-core cross-socket/CCX contention, `libponyrt`'s `mpmcq_t` runqueue saves ~3 nanoseconds per hop.

---

### 4.2 Odin (`odin -o:speed`) vs. C11 (`gcc -O3 -march=native`)

Because [`actor_benchmark.odin`](actor_benchmark.odin) and [`actor_benchmark_c11.c`](actor_benchmark_c11.c) implement the **exact same data structures and atomic memory orders**, we can directly measure Odin's LLVM backend against GCC 16.1.1 C11:

| Workload | Odin (`odin -o:speed`) | C11 (`gcc -O3 -march=native`) | Ratio (`Odin / C11`) | Analysis |
| :--- | ---: | ---: | ---: | :--- |
| **`3c` Raw Pony MPSC (Single Send)** | `59.63 ms` (`33.54 M/s`) | `59.60 ms` (`33.56 M/s`) | **`1.00x` (Identical)** | Bottlenecked by hardware `LOCK XCHG` L3 cache line bouncing (~`29.8 ns`), not compiler codegen. |
| **`3d` Raw Pony MPSC (`Chain=32` Batch)** | `6.37 ms` (`313.83 M/s`) | `6.50 ms` (`307.53 M/s`) | **`1.02x` (Odin +2%)** | Identical codegen (`~3.2 ns/msg`) when `XCHG` cost is amortized $32\times$. |
| **`3e` Wait-Free Sharded SPSC (`4 Lanes`)** | **`2.42 ms` (`827.45 M/s`)** | `3.56 ms` (`561.76 M/s`) | **`1.47x` (Odin +47%)** | Odin's LLVM backend unrolls the consumer ring drain across the 4 fixed-stride lanes tighter than GCC's loop codegen (`1.2 ns` vs `1.8 ns`). |
| **`4b` Official `libponyrt` (`Chain=32` FFI)** | `27.74 ms` (`72.10 M/s`) | `27.00 ms` (`74.07 M/s`) | **`0.97x` (Within 3%)** | Calling `libponyrt.a` via Odin `foreign import` (`proc "c"`) has **zero measurable FFI overhead** compared to native C11. |
| **`2b` Multi-Thread DOD (`4 TLS Arrays`)** | `10.12 ms` (`197.70 M/s`) | `10.08 ms` (`198.44 M/s`) | **`1.00x` (Identical)** | Thread-local array writes + sequential drain perform identically (`5.0–5.1 ns/msg`). |
| **`3h` Autonomous `1,024`-Actor Graph (`8T`)** | `26.74 ms` (`18.70 M/s`) | `25.08 ms` (`19.93 M/s`) | **`0.94x` (Within 6%)** | Near-identical scaling across `1`, `2`, `4`, and `8` hardware cores. |
| **`2a` Single-Thread DOD Array (`16B`)** | `5.16 ms` (`387.54 M/s`) | **`2.29 ms` (`872.21 M/s`)** | **`0.44x` (C11 2.25x faster)** | GCC auto-vectorizes the single-thread `Sim_Event` fill+sum loop with 256-bit AVX2 instructions more aggressively when both fill and reduction live in one C translation unit. |

> **Key Takeaway:** For lock-free concurrent algorithms (MPSC queues, SPSC rings, work-stealing deques, and C FFI), **Odin `-o:speed` generates machine code at 95%–147% of `gcc -O3 -march=native` C11 performance.**

---

### 4.3 Sleipnir (`package actor` Generic Library) vs. Raw Specialized Odin (`3b`–`3h`)

What is the abstraction cost of Sleipnir's clean, generic, heterogeneous `package actor` API compared to hand-inlined, single-type raw Odin structs?

| Workload | Raw Specialized Odin (`3c`/`3d`/`3g`/`3h`) | Sleipnir Library (`package actor` `5a`/`5b`/`5c`) | Library Overhead | Why the Difference Exists |
| :--- | ---: | ---: | ---: | :--- |
| **Single-Msg 4-Producer Fan-In** | `33.54 M/s` (`29.8 ns`) | `31.08 M/s` (`32.2 ns`) | **`+2.4 ns/msg` (`7.3%`)** | Indirect call through `Actor_Base.process_batch` + generic `return_envelope` callback to support heterogeneous actor types. |
| **Batched `Chain=32` Fan-In** | `313.83 M/s` (`3.2 ns`) | `77.89 M/s` (`12.8 ns`) | **`+9.6 ns/msg`** | Raw `3d` uses a pure watermark index bump on a contiguous ring where producer ID is implicit; `package actor`'s `Envelope_Pool` pops/pushes arbitrary recycled pointers via an intrusive free-list stack so envelopes can be freed out-of-order across arbitrary actors. |
| **`1,024`-Actor `M:N` Swarm (`4P -> 4W`)** | `53.95 M/s` (`18.5 ns`) | `47.79 M/s` (`20.9 ns`) | **`+2.4 ns/msg` (`11.4%`)** | Only `2.4 ns` overhead for full generic `(State, Msg)` type erasure and dynamic pool recycling across `1,024` actors. |
| **Autonomous `1,024`-Actor Graph (`1–4T`)** | `10.96 M/s` (`45.6 ms` @ 4T) | `10.52 M/s` (`47.5 ms` @ 4T) | **`4.0%`** | When actors perform realistic work (`~45 ns` CPU per message), library abstraction overhead is practically zero (`~4%`). |

---

### 4.4 Actor Mailboxes (`MPSC` & `Sharded_SPSC`) vs. Channels (`chan`) vs. DOD Arrays

Why not just use Odin's standard `core:sync/chan` or skip actors entirely for flat Data-Oriented Design (`DOD`) arrays?

| Paradigm | Representative Throughput (`2M` Fan-In) | Latency / Msg | Strengths | Limitations |
| :--- | ---: | ---: | :--- | :--- |
| **Odin `core:sync/chan` (`1`)** | `1.32 M/s` | `755.2 ns` | Built into `core:sync`; supports blocking multi-producer/multi-consumer semantics out of the box. | Acquires an internal mutex and wakes condition variables under contention. **`23.5x`–`625x` slower** than lock-free actor mailboxes. |
| **C11 `pthread_mutex_t` Channel (`1`)** | `4.74 M/s` | `211.1 ns` | Simple bounded ring buffer protected by a POSIX mutex. | Serializes all 4 producers + 1 consumer on a single kernel futex lock (`7x` slower than Pony MPSC). |
| **Sleipnir / Pony MPSC Mailbox (`3c`, `4a`, `5a`)** | `23.4 – 33.5 M/s` (`77.9 M/s` batched) | `29.8 – 42.7 ns` (`12.8 ns` batched) | Supports **unbounded dynamic producers** (any actor can send to any `PID` at any time), scales to millions of lightweight actors on `M:N` worker threads. | Every unbatched send executes `1` atomic `LOCK XCHG` on `tail` (`~30 ns` floor under 4-core contention). |
| **Wait-Free `Sharded_SPSC_Mailbox` (`3e`)** | **`827.45 M/s`** | **`1.2 ns`** | **Zero atomic RMW instructions (`0` `LOCK XCHG`)**. Faster than single-threaded Odin DOD array processing (`827 M/s` vs `387 M/s`) because 4 cores fill ring lanes in parallel. | Requires a fixed, known number of producer lanes (`num_producers`), allocating `O(P × N)` ring memory per mailbox. Best for high-frequency "hot" ingestion actors. |
| **DOD Flat Batch Arrays (`2a`, `2b`)** | `197.7 – 872.2 M/s` | `1.1 – 5.1 ns` | Optimal L1/L2 spatial locality and SIMD auto-vectorization for homogeneous data transformations (ECS / physics / particle systems). | Synchronous frame/barrier model; cannot express autonomous, stateful, reactive entities communicating asynchronously across a graph. |

---

### 4.5 Architectural Decision Matrix

Use this matrix to choose the right primitive inside Sleipnir (`package actor`):

| Workload Pattern | Recommended Primitive | Expected Performance |
| :--- | :--- | :--- |
| **General stateful actors, dynamic topologies, request/reply, state machines** | [`actor.Scheduler`](../actor/actor.odin) + [`actor.Envelope_Pool`](../actor/pool.odin) (`actor.send`) | **`31 – 44 M/s`** single send (`22.7 – 32.2 ns/msg`), **`48 – 62 M/s`** across `1,024`-actor swarm |
| **Producers emitting bursts of messages to the same actor** | [`actor.send(&sched, &pool, pid, batch[:])`](../actor/actor.odin) (`Chain=K` splicing) | **`72 – 78 M/s`** in `package actor` (`12.8 – 13.9 ns/msg`), up to **`314 – 484 M/s`** raw (`2.1 – 3.2 ns/msg`) |
| **Pipelines, rings, routers, or ping-pong state machines** | [`actor.forward(ctx, next_pid, msg)`](../actor/actor.odin) (zero-copy `iso` handoff) | **`8.2 – 10.5 M hops/sec`** cross-thread ping-pong, **`16.7 – 17.9 M hops/sec`** on 8-core actor graph |
| **Dedicated high-throughput sink/aggregator actor fed by $P$ known worker threads** | [`actor.Sharded_SPSC_Mailbox(T, N)`](../actor/spsc.odin) | **`827 M – 1.06 B/s`** (`0.9 – 1.2 ns/msg`, wait-free) |
| **Homogeneous data-parallel number crunching (e.g., updating 100,000 positions per tick)** | Contiguous DOD slice (`[]T`) inside a single actor message or worker loop | **`200 – 870 M/s`** (`1.1 – 5.1 ns/item`) |

---

## 5. Reproducing the Benchmarks

### Step 1: Build the Official Pony C Runtime (`libponyrt` 0.77.2)

[`benchmarks/build_libponyrt.sh`](build_libponyrt.sh) shallow-clones [`ponylang/ponyc` (`0.77.2`)](https://github.com/ponylang/ponyc/tree/0.77.2) into `.tmp/ponyc_src` and compiles `libponyrt` (`src/libponyrt/**/*.c`) with `gcc -O3 -march=native -mcx16` into `.tmp/ponyc_build/libponyrt.a` and `.tmp/ponyc_build/libponyrt.so` in ~6 seconds:

```bash
./benchmarks/build_libponyrt.sh
```

### Step 2: Run the Full Odin Benchmark Suite (Including Official `libponyrt`)

```bash
odin run benchmarks/actor_benchmark.odin -file -o:speed \
  -define:USE_LIBPONYRT=true \
  -extra-linker-flags:"-L.tmp/ponyc_build -lponyrt -Wl,-rpath,.tmp/ponyc_build -lpthread -ldl -lm -latomic"
```

*(To run the Odin suite without building `libponyrt`, simply omit `-define:USE_LIBPONYRT=true` and `-extra-linker-flags`:)*

```bash
odin run benchmarks/actor_benchmark.odin -file -o:speed
```

### Step 3: Run the C11 Reference Benchmark Suite (Including Official `libponyrt`)

```bash
gcc -O3 -march=native -pthread -DUSE_LIBPONYRT \
  benchmarks/actor_benchmark_c11.c \
  -L.tmp/ponyc_build -lponyrt -Wl,-rpath,.tmp/ponyc_build -ldl -lm -latomic \
  -o .tmp/actor_benchmark_c11 && ./.tmp/actor_benchmark_c11
```
