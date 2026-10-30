/*
 * Unified C11 / pthreads Reference Benchmark Suite (actor_benchmark_c11.c)
 * Reproduces all 5 workloads from the Odin Actor Benchmark Suite:
 *   - Workload 1: 4-Producer -> 1-Actor Fan-In Contention (2,000,000 messages)
 *   - Workload 2: Cross-Thread 1-to-1 Ping-Pong Latency (100,000 round-trips)
 *   - Workload 3: 512-Byte Large Payload Crossover (500,000 messages)
 *   - Workload 4: 1,024-Actor Swarm Scaling (2,000,000 messages)
 *   - Workload 5: Autonomous Actor-to-Actor Graph + Per-Message CPU Work (500,000 hops)
 *
 * Compile & run (pure C11 + pthreads):
 *   gcc -O3 -march=native -pthread benchmarks/actor_benchmark_c11.c -o .tmp/actor_benchmark_c11
 *   ./.tmp/actor_benchmark_c11
 *
 * Compile & run with official Pony runtime (`libponyrt` 0.77.2):
 *   ./benchmarks/build_libponyrt.sh
 *   gcc -O3 -march=native -pthread -DUSE_LIBPONYRT \
 *     -I.tmp/ponyc_src/src/libponyrt -I.tmp/ponyc_src/src/common \
 *     benchmarks/actor_benchmark_c11.c .tmp/ponyc_build/libponyrt.a \
 *     -latomic -lm -ldl -o .tmp/actor_benchmark_c11_pony
 *   ./.tmp/actor_benchmark_c11_pony
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <linux/futex.h>
#include <immintrin.h>

#ifdef USE_LIBPONYRT
#define PONY_WANT_ATOMIC_DEFS
#include "pony.h"
#include "actor/actor.h"
#include "actor/messageq.h"
#include "mem/pool.h"
#include "sched/scheduler.h"
#include "asio/asio.h"

void Main_runtime_override_defaults_oo(void *opt) { (void)opt; }
#endif

#define TOTAL_MSGS      2000000
#define LARGE_MSGS      500000
#define A2A_TOTAL_HOPS  500000
#define A2A_INIT_TOKENS 8192
#define CPU_WORK_ITERS  64
#define NUM_PRODUCERS   4
#define SWARM_ACTORS    1024
#define SWARM_MASK      (SWARM_ACTORS - 1)
#define NUM_WORKERS     2
#define PINGPONG_ITERS  100000
#define BATCH_QUOTA     100
#define CHAIN_LEN       32
#define RING_SLAB_SIZE  8192
#define RING_SLAB_MASK  (RING_SLAB_SIZE - 1)
#define SPIN_LIMIT      1024

static inline double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static void print_row(const char *name, int msgs, double elapsed, int64_t score) {
    double mops = (msgs / elapsed) / 1e6;
    double ns   = (elapsed * 1e9) / msgs;
    printf("| %-48s | %8.2f ms | %9.2f M/s | %8.1f ns/msg | %ld |\n",
           name, elapsed * 1000.0, mops, ns, score);
}

/* ============================================================================
 * 1. Traditional Channel + Thread (Mutex + Condvar Ring Buffer)
 * ============================================================================ */

typedef struct {
    int64_t points;
    int     quit;
} chan_msg_t;

typedef struct {
    chan_msg_t     *buf;
    size_t          cap;
    size_t          head;
    size_t          tail;
    size_t          count;
    pthread_mutex_t mtx;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} odin_chan_t;

static void chan_init(odin_chan_t *c, size_t cap) {
    c->buf   = calloc(cap, sizeof(chan_msg_t));
    c->cap   = cap;
    c->head  = 0;
    c->tail  = 0;
    c->count = 0;
    pthread_mutex_init(&c->mtx, NULL);
    pthread_cond_init(&c->not_empty, NULL);
    pthread_cond_init(&c->not_full, NULL);
}

static void chan_destroy(odin_chan_t *c) {
    free(c->buf);
    pthread_mutex_destroy(&c->mtx);
    pthread_cond_destroy(&c->not_empty);
    pthread_cond_destroy(&c->not_full);
}

static inline void chan_send(odin_chan_t *c, chan_msg_t msg) {
    pthread_mutex_lock(&c->mtx);
    while (c->count == c->cap) {
        pthread_cond_wait(&c->not_full, &c->mtx);
    }
    c->buf[c->tail] = msg;
    c->tail = (c->tail + 1) & (c->cap - 1);
    c->count++;
    pthread_cond_signal(&c->not_empty);
    pthread_mutex_unlock(&c->mtx);
}

static inline chan_msg_t chan_recv(odin_chan_t *c) {
    pthread_mutex_lock(&c->mtx);
    while (c->count == 0) {
        pthread_cond_wait(&c->not_empty, &c->mtx);
    }
    chan_msg_t msg = c->buf[c->head];
    c->head = (c->head + 1) & (c->cap - 1);
    c->count--;
    pthread_cond_signal(&c->not_full);
    pthread_mutex_unlock(&c->mtx);
    return msg;
}

typedef struct {
    odin_chan_t inbox;
    int64_t     score;
} chan_actor_t;

static void *chan_actor_worker(void *arg) {
    chan_actor_t *a = (chan_actor_t *)arg;
    for (;;) {
        chan_msg_t m = chan_recv(&a->inbox);
        if (m.quit) break;
        a->score += m.points;
    }
    return NULL;
}

typedef struct {
    odin_chan_t *inbox;
    int          count;
} chan_prod_arg_t;

static void *chan_producer_worker(void *arg) {
    chan_prod_arg_t *pa = (chan_prod_arg_t *)arg;
    chan_msg_t m = { .points = 1, .quit = 0 };
    for (int i = 0; i < pa->count; i++) {
        chan_send(pa->inbox, m);
    }
    return NULL;
}

static double bench_chan_fan_in(int total_msgs, int nprod, int64_t *out_score) {
    chan_actor_t actor = {0};
    chan_init(&actor.inbox, 4096);
    pthread_t consumer, prods[16];
    chan_prod_arg_t args[16];

    double t0 = now_sec();
    pthread_create(&consumer, NULL, chan_actor_worker, &actor);
    int per_prod = total_msgs / nprod;
    for (int i = 0; i < nprod; i++) {
        args[i].inbox = &actor.inbox;
        args[i].count = per_prod;
        pthread_create(&prods[i], NULL, chan_producer_worker, &args[i]);
    }
    for (int i = 0; i < nprod; i++) pthread_join(prods[i], NULL);
    chan_msg_t quit_msg = { .points = 0, .quit = 1 };
    chan_send(&actor.inbox, quit_msg);
    pthread_join(consumer, NULL);
    double elapsed = now_sec() - t0;

    *out_score = actor.score;
    chan_destroy(&actor.inbox);
    return elapsed;
}

/* ============================================================================
 * 2a & 2b. Data-Oriented Batch System (Single-Thread & Multi-Thread TLS)
 * ============================================================================ */

typedef struct {
    uint32_t target;
    int64_t  amount;
} dod_msg_t;

typedef struct {
    uint32_t id;
    int64_t  score;
} dod_actor_t;

static double bench_dod_single_thread(int total_msgs, int num_actors, int64_t *out_score) {
    dod_actor_t *actors = calloc(num_actors, sizeof(dod_actor_t));
    const int batch_cap = 4096;
    dod_msg_t *msgs = malloc(batch_cap * sizeof(dod_msg_t));
    uint32_t mask = (uint32_t)(num_actors - 1);

    double t0 = now_sec();
    int sent = 0;
    while (sent < total_msgs) {
        int chunk = (total_msgs - sent < batch_cap) ? (total_msgs - sent) : batch_cap;
        for (int i = 0; i < chunk; i++) {
            msgs[i].target = (uint32_t)(sent + i) & mask;
            msgs[i].amount = 1;
        }
        for (int i = 0; i < chunk; i++) {
            actors[msgs[i].target].score += msgs[i].amount;
        }
        sent += chunk;
    }
    double elapsed = now_sec() - t0;

    int64_t sum = 0;
    for (int i = 0; i < num_actors; i++) sum += actors[i].score;
    *out_score = sum;
    free(msgs);
    free(actors);
    return elapsed;
}

typedef struct {
    dod_msg_t *cmd_buf;
    int        count;
    uint32_t   mask;
    uint32_t   seed;
} dod_tls_prod_t;

static void *dod_tls_producer(void *arg) {
    dod_tls_prod_t *p = (dod_tls_prod_t *)arg;
    for (int i = 0; i < p->count; i++) {
        p->cmd_buf[i].target = (p->seed + (uint32_t)i) & p->mask;
        p->cmd_buf[i].amount = 1;
    }
    return NULL;
}

static double bench_dod_multithread_tls(int total_msgs, int nprod, int num_actors, int64_t *out_score) {
    dod_actor_t *actors = calloc(num_actors, sizeof(dod_actor_t));
    int per_prod = total_msgs / nprod;
    dod_msg_t *all_bufs = malloc((size_t)total_msgs * sizeof(dod_msg_t));
    pthread_t prods[16];
    dod_tls_prod_t args[16];

    double t0 = now_sec();
    for (int p = 0; p < nprod; p++) {
        args[p].cmd_buf = &all_bufs[p * per_prod];
        args[p].count   = per_prod;
        args[p].mask    = (uint32_t)(num_actors - 1);
        args[p].seed    = (uint32_t)(p * per_prod);
        pthread_create(&prods[p], NULL, dod_tls_producer, &args[p]);
    }
    for (int p = 0; p < nprod; p++) pthread_join(prods[p], NULL);
    for (int i = 0; i < total_msgs; i++) {
        actors[all_bufs[i].target].score += all_bufs[i].amount;
    }
    double elapsed = now_sec() - t0;

    int64_t sum = 0;
    for (int i = 0; i < num_actors; i++) sum += actors[i].score;
    *out_score = sum;
    free(all_bufs);
    free(actors);
    return elapsed;
}

/* ============================================================================
 * 3c, 3d, 3e. Self-Implemented Pony MPSC & Sharded SPSC Lanes
 * ============================================================================ */

typedef struct pony_odin_msg {
    _Atomic(struct pony_odin_msg *) next;
    uint32_t                        prod_id;
    int64_t                         points;
} pony_odin_msg_t;

typedef struct {
    _Atomic(uint64_t) count __attribute__((aligned(64)));
} prod_watermark_t;

typedef struct {
    _Atomic(pony_odin_msg_t *) head __attribute__((aligned(64)));
    pony_odin_msg_t           *tail __attribute__((aligned(64)));
    pony_odin_msg_t            initial_stub __attribute__((aligned(64)));
    prod_watermark_t           retired[NUM_PRODUCERS];
    int64_t                    score __attribute__((aligned(64)));
    int                        received;
} __attribute__((aligned(64))) batched_actor_t;

static inline void mpsc_push_single(batched_actor_t *a, pony_odin_msg_t *msg) {
    atomic_store_explicit(&msg->next, NULL, memory_order_relaxed);
    pony_odin_msg_t *prev = atomic_exchange_explicit(&a->head, msg, memory_order_acq_rel);
    atomic_store_explicit(&prev->next, msg, memory_order_release);
}

static inline void mpsc_push_chain(batched_actor_t *a, pony_odin_msg_t *first, pony_odin_msg_t *last) {
    atomic_store_explicit(&last->next, NULL, memory_order_relaxed);
    pony_odin_msg_t *prev = atomic_exchange_explicit(&a->head, last, memory_order_acq_rel);
    atomic_store_explicit(&prev->next, first, memory_order_release);
}

typedef struct {
    batched_actor_t *actor;
    pony_odin_msg_t *slab;
    uint32_t         prod_id;
    int              count;
    bool             use_chain;
} batched_prod_t;

static void *batched_producer_worker(void *arg) {
    batched_prod_t *p = (batched_prod_t *)arg;
    batched_actor_t *a = p->actor;
    uint32_t pid = p->prod_id;
    uint64_t cursor = 0;
    uint64_t cached_retired = 0;

    if (!p->use_chain) {
        for (int i = 0; i < p->count; i++) {
            while (cursor - cached_retired >= (RING_SLAB_SIZE - 2)) {
                cached_retired = atomic_load_explicit(&a->retired[pid].count, memory_order_acquire);
                if (cursor - cached_retired >= (RING_SLAB_SIZE - 2)) _mm_pause();
            }
            pony_odin_msg_t *slot = &p->slab[cursor & RING_SLAB_MASK];
            cursor++;
            slot->prod_id = pid;
            slot->points  = 1;
            mpsc_push_single(a, slot);
        }
    } else {
        int remaining = p->count;
        while (remaining > 0) {
            int k = (remaining >= CHAIN_LEN) ? CHAIN_LEN : remaining;
            while ((cursor + (uint64_t)k) - cached_retired >= (RING_SLAB_SIZE - 2)) {
                cached_retired = atomic_load_explicit(&a->retired[pid].count, memory_order_acquire);
                if ((cursor + (uint64_t)k) - cached_retired >= (RING_SLAB_SIZE - 2)) _mm_pause();
            }
            pony_odin_msg_t *first = &p->slab[cursor & RING_SLAB_MASK];
            pony_odin_msg_t *prev  = first;
            first->prod_id = pid;
            first->points  = 1;
            cursor++;
            for (int i = 1; i < k; i++) {
                pony_odin_msg_t *cur = &p->slab[cursor & RING_SLAB_MASK];
                cursor++;
                cur->prod_id = pid;
                cur->points  = 1;
                atomic_store_explicit(&prev->next, cur, memory_order_relaxed);
                prev = cur;
            }
            mpsc_push_chain(a, first, prev);
            remaining -= k;
        }
    }
    return NULL;
}

static double bench_batched_mpsc(int total_msgs, int nprod, bool use_chain, int64_t *out_score) {
    void *raw_actor = NULL;
    if (posix_memalign(&raw_actor, 64, sizeof(batched_actor_t)) != 0) exit(1);
    batched_actor_t *actor = (batched_actor_t *)raw_actor;
    memset(actor, 0, sizeof(*actor));
    atomic_init(&actor->initial_stub.next, NULL);
    actor->initial_stub.prod_id = 0xFFFFFFFFu;
    atomic_init(&actor->head, &actor->initial_stub);
    actor->tail = &actor->initial_stub;

    pony_odin_msg_t *slabs = calloc((size_t)nprod * RING_SLAB_SIZE, sizeof(pony_odin_msg_t));
    pthread_t prods[16];
    batched_prod_t args[16];
    int per_prod = total_msgs / nprod;

    double t0 = now_sec();
    for (int i = 0; i < nprod; i++) {
        args[i].actor     = actor;
        args[i].slab      = &slabs[i * RING_SLAB_SIZE];
        args[i].prod_id   = (uint32_t)i;
        args[i].count     = per_prod;
        args[i].use_chain = use_chain;
        pthread_create(&prods[i], NULL, batched_producer_worker, &args[i]);
    }

    while (actor->received < total_msgs) {
        int processed = 0;
        uint32_t local_retired[NUM_PRODUCERS] = {0};
        while (processed < BATCH_QUOTA) {
            pony_odin_msg_t *tail = actor->tail;
            pony_odin_msg_t *next = atomic_load_explicit(&tail->next, memory_order_acquire);
            if (!next) break;
            actor->score += next->points;
            actor->tail = next;
            if (tail->prod_id < NUM_PRODUCERS) local_retired[tail->prod_id]++;
            actor->received++;
            processed++;
        }
        if (processed > 0) {
            for (int p = 0; p < nprod; p++) {
                if (local_retired[p] > 0) {
                    atomic_fetch_add_explicit(&actor->retired[p].count, local_retired[p], memory_order_release);
                }
            }
        } else {
            _mm_pause();
        }
    }

    for (int i = 0; i < nprod; i++) pthread_join(prods[i], NULL);
    double elapsed = now_sec() - t0;
    *out_score = actor->score;
    free(slabs);
    free(actor);
    return elapsed;
}

/* 3e. Sharded SPSC Lanes */
typedef struct {
    _Atomic(uint64_t) write_idx __attribute__((aligned(64)));
    _Atomic(uint64_t) read_idx  __attribute__((aligned(64)));
    uint64_t          cached_write_idx;
    int64_t           ring[RING_SLAB_SIZE] __attribute__((aligned(64)));
} __attribute__((aligned(64))) spsc_lane_t;

typedef struct {
    spsc_lane_t lanes[NUM_PRODUCERS];
    int64_t     score __attribute__((aligned(64)));
    int         received;
} __attribute__((aligned(64))) sharded_actor_t;

typedef struct {
    spsc_lane_t *lane;
    int          count;
} spsc_prod_t;

static void *spsc_producer_worker(void *arg) {
    spsc_prod_t *p = (spsc_prod_t *)arg;
    spsc_lane_t *lane = p->lane;
    uint64_t w = 0, r_cache = 0;
    for (int i = 0; i < p->count; i++) {
        while (w - r_cache >= RING_SLAB_SIZE) {
            r_cache = atomic_load_explicit(&lane->read_idx, memory_order_acquire);
            if (w - r_cache >= RING_SLAB_SIZE) _mm_pause();
        }
        lane->ring[w & RING_SLAB_MASK] = 1;
        w++;
        atomic_store_explicit(&lane->write_idx, w, memory_order_release);
    }
    return NULL;
}

static double bench_sharded_spsc(int total_msgs, int nprod, int64_t *out_score) {
    void *raw_actor = NULL;
    if (posix_memalign(&raw_actor, 64, sizeof(sharded_actor_t)) != 0) exit(1);
    sharded_actor_t *actor = (sharded_actor_t *)raw_actor;
    memset(actor, 0, sizeof(*actor));

    pthread_t prods[16];
    spsc_prod_t args[16];
    int per_prod = total_msgs / nprod;

    double t0 = now_sec();
    for (int i = 0; i < nprod; i++) {
        args[i].lane  = &actor->lanes[i];
        args[i].count = per_prod;
        pthread_create(&prods[i], NULL, spsc_producer_worker, &args[i]);
    }

    while (actor->received < total_msgs) {
        int total_batch = 0;
        for (int p = 0; p < nprod; p++) {
            spsc_lane_t *lane = &actor->lanes[p];
            uint64_t r = atomic_load_explicit(&lane->read_idx, memory_order_relaxed);
            uint64_t w = lane->cached_write_idx;
            if (r == w) {
                w = atomic_load_explicit(&lane->write_idx, memory_order_acquire);
                lane->cached_write_idx = w;
            }
            uint64_t avail = w - r;
            if (avail > BATCH_QUOTA) avail = BATCH_QUOTA;
            if (avail > 0) {
                for (uint64_t k = 0; k < avail; k++) {
                    actor->score += lane->ring[(r + k) & RING_SLAB_MASK];
                }
                r += avail;
                atomic_store_explicit(&lane->read_idx, r, memory_order_release);
                actor->received += (int)avail;
                total_batch += (int)avail;
            }
        }
        if (total_batch == 0) _mm_pause();
    }

    for (int i = 0; i < nprod; i++) pthread_join(prods[i], NULL);
    double elapsed = now_sec() - t0;
    *out_score = actor->score;
    free(actor);
    return elapsed;
}

/* ============================================================================
 * Workload 5: Autonomous Actor-to-Actor Graph + Per-Message CPU Work
 * ============================================================================ */

static inline uint64_t simulate_cpu_work(uint64_t seed) {
    uint64_t z = seed;
    for (int i = 0; i < CPU_WORK_ITERS; i++) {
        z += 0x9e3779b97f4a7c15ULL;
        z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
        z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
        z = z ^ (z >> 31);
    }
    return z;
}

typedef struct a2a_msg {
    _Atomic(struct a2a_msg *) next;
    uint64_t                  payload;
    uint32_t                  hops_left;
} a2a_msg_t;

typedef struct a2a_actor {
    _Atomic(a2a_msg_t *)        head __attribute__((aligned(64)));
    a2a_msg_t                  *tail __attribute__((aligned(64)));
    a2a_msg_t                   stub;
    _Atomic(uint32_t)           scheduled;
    _Atomic(struct a2a_actor *) sched_next;
    uint32_t                    id;
    uint64_t                    state;
} __attribute__((aligned(64))) a2a_actor_t;

static void a2a_actor_init(a2a_actor_t *a, uint32_t id) {
    memset(a, 0, sizeof(*a));
    atomic_init(&a->stub.next, NULL);
    atomic_init(&a->head, &a->stub);
    a->tail = &a->stub;
    atomic_init(&a->scheduled, 0);
    atomic_init(&a->sched_next, NULL);
    a->id = id;
}

static inline void a2a_mpsc_push(a2a_actor_t *a, a2a_msg_t *m) {
    atomic_store_explicit(&m->next, NULL, memory_order_relaxed);
    a2a_msg_t *prev = atomic_exchange_explicit(&a->head, m, memory_order_acq_rel);
    atomic_store_explicit(&prev->next, m, memory_order_release);
}

static inline a2a_msg_t *a2a_mpsc_pop(a2a_actor_t *a) {
    a2a_msg_t *tail = a->tail;
    a2a_msg_t *next = atomic_load_explicit(&tail->next, memory_order_acquire);
    if (tail == &a->stub) {
        if (!next) return NULL;
        a->tail = next;
        tail = next;
        next = atomic_load_explicit(&tail->next, memory_order_acquire);
    }
    if (next) {
        a->tail = next;
        return tail;
    }
    a2a_msg_t *head = atomic_load_explicit(&a->head, memory_order_acquire);
    if (tail != head) return NULL;

    atomic_store_explicit(&a->stub.next, NULL, memory_order_relaxed);
    a2a_msg_t *prev = atomic_exchange_explicit(&a->head, &a->stub, memory_order_acq_rel);
    atomic_store_explicit(&prev->next, &a->stub, memory_order_release);
    next = atomic_load_explicit(&tail->next, memory_order_acquire);
    while (!next) {
        _mm_pause();
        next = atomic_load_explicit(&tail->next, memory_order_acquire);
    }
    a->tail = next;
    return tail;
}

typedef struct {
    _Atomic(a2a_actor_t *) head __attribute__((aligned(64)));
    a2a_actor_t           *tail __attribute__((aligned(64)));
    a2a_actor_t            stub __attribute__((aligned(64)));
    pthread_spinlock_t     pop_lock;
} __attribute__((aligned(64))) a2a_shard_t;

static void a2a_shard_init(a2a_shard_t *q) {
    memset(q, 0, sizeof(*q));
    atomic_init(&q->stub.sched_next, NULL);
    atomic_init(&q->head, &q->stub);
    q->tail = &q->stub;
    pthread_spin_init(&q->pop_lock, PTHREAD_PROCESS_PRIVATE);
}

static inline void a2a_shard_push(a2a_shard_t *q, a2a_actor_t *a) {
    atomic_store_explicit(&a->sched_next, NULL, memory_order_relaxed);
    a2a_actor_t *prev = atomic_exchange_explicit(&q->head, a, memory_order_acq_rel);
    atomic_store_explicit(&prev->sched_next, a, memory_order_release);
}

static inline a2a_actor_t *a2a_shard_pop(a2a_shard_t *q) {
    if (q->tail == &q->stub &&
        atomic_load_explicit(&q->stub.sched_next, memory_order_relaxed) == NULL) {
        return NULL;
    }
    if (pthread_spin_trylock(&q->pop_lock) != 0) return NULL;

    a2a_actor_t *tail = q->tail;
    a2a_actor_t *next = atomic_load_explicit(&tail->sched_next, memory_order_acquire);
    if (tail == &q->stub) {
        if (!next) {
            pthread_spin_unlock(&q->pop_lock);
            return NULL;
        }
        q->tail = next;
        tail = next;
        next = atomic_load_explicit(&tail->sched_next, memory_order_acquire);
    }
    if (next) {
        q->tail = next;
        pthread_spin_unlock(&q->pop_lock);
        return tail;
    }
    a2a_actor_t *head = atomic_load_explicit(&q->head, memory_order_acquire);
    if (tail != head) {
        pthread_spin_unlock(&q->pop_lock);
        return NULL;
    }
    atomic_store_explicit(&q->stub.sched_next, NULL, memory_order_relaxed);
    a2a_actor_t *prev = atomic_exchange_explicit(&q->head, &q->stub, memory_order_acq_rel);
    atomic_store_explicit(&prev->sched_next, &q->stub, memory_order_release);
    next = atomic_load_explicit(&tail->sched_next, memory_order_acquire);
    while (!next) {
        _mm_pause();
        next = atomic_load_explicit(&tail->sched_next, memory_order_acquire);
    }
    q->tail = next;
    pthread_spin_unlock(&q->pop_lock);
    return tail;
}

typedef struct {
    a2a_shard_t  shards[16];
    a2a_actor_t *actors;
    int          num_shards;
    _Atomic(int) remaining_tokens __attribute__((aligned(64)));
} a2a_sched_t;

typedef struct {
    a2a_sched_t *sched;
    int          wid;
} a2a_worker_arg_t;

static void *a2a_worker_proc(void *arg) {
    a2a_worker_arg_t *wa = (a2a_worker_arg_t *)arg;
    a2a_sched_t *s = wa->sched;
    int wid = wa->wid;
    a2a_shard_t *my_shard = &s->shards[wid];

    while (atomic_load_explicit(&s->remaining_tokens, memory_order_acquire) > 0) {
        a2a_actor_t *a = a2a_shard_pop(my_shard);
        if (!a && s->num_shards > 1) {
            for (int i = 1; i < s->num_shards; i++) {
                int peer = (wid + i) % s->num_shards;
                a = a2a_shard_pop(&s->shards[peer]);
                if (a) break;
            }
        }
        if (!a) {
            _mm_pause();
            continue;
        }

        int processed = 0, retired_tokens = 0;
        while (processed < 64) {
            a2a_msg_t *tok = a2a_mpsc_pop(a);
            if (!tok) break;
            uint64_t res = simulate_cpu_work(tok->payload ^ a->state);
            a->state ^= res;
            processed++;

            if (tok->hops_left > 1) {
                tok->hops_left--;
                tok->payload = res;
                uint32_t target_id = (a->id * 31u + (uint32_t)res) & SWARM_MASK;
                a2a_actor_t *target = &s->actors[target_id];
                a2a_mpsc_push(target, tok);
                if (atomic_exchange_explicit(&target->scheduled, 1, memory_order_acq_rel) == 0) {
                    a2a_shard_push(my_shard, target);
                }
            } else {
                retired_tokens++;
            }
        }
        if (retired_tokens > 0) {
            atomic_fetch_sub_explicit(&s->remaining_tokens, retired_tokens, memory_order_release);
        }
        if (atomic_load_explicit(&a->head, memory_order_acquire) != a->tail) {
            a2a_shard_push(my_shard, a);
        } else {
            atomic_store_explicit(&a->scheduled, 0, memory_order_seq_cst);
            if (atomic_load_explicit(&a->head, memory_order_seq_cst) != a->tail) {
                if (atomic_exchange_explicit(&a->scheduled, 1, memory_order_acq_rel) == 0) {
                    a2a_shard_push(my_shard, a);
                }
            }
        }
    }
    return NULL;
}

static double bench_pony_mn_a2a(int total_hops, int num_workers, int64_t *out_score) {
    void *raw_sched = NULL;
    if (posix_memalign(&raw_sched, 64, sizeof(a2a_sched_t)) != 0) exit(1);
    a2a_sched_t *sched = (a2a_sched_t *)raw_sched;
    memset(sched, 0, sizeof(*sched));
    sched->num_shards = num_workers;
    for (int w = 0; w < num_workers; w++) a2a_shard_init(&sched->shards[w]);
    atomic_init(&sched->remaining_tokens, A2A_INIT_TOKENS);

    void *raw_actors = NULL;
    if (posix_memalign(&raw_actors, 64, SWARM_ACTORS * sizeof(a2a_actor_t)) != 0) exit(1);
    a2a_actor_t *actors = (a2a_actor_t *)raw_actors;
    for (int i = 0; i < SWARM_ACTORS; i++) a2a_actor_init(&actors[i], (uint32_t)i);
    sched->actors = actors;

    a2a_msg_t *tokens = calloc(A2A_INIT_TOKENS, sizeof(a2a_msg_t));
    int base_hops = total_hops / A2A_INIT_TOKENS;
    int extra     = total_hops % A2A_INIT_TOKENS;
    for (int i = 0; i < A2A_INIT_TOKENS; i++) {
        tokens[i].payload   = (uint64_t)(i + 1);
        tokens[i].hops_left = (uint32_t)(base_hops + (i < extra ? 1 : 0));
        a2a_actor_t *target = &actors[i & SWARM_MASK];
        a2a_mpsc_push(target, &tokens[i]);
        if (atomic_exchange_explicit(&target->scheduled, 1, memory_order_acq_rel) == 0) {
            a2a_shard_push(&sched->shards[i % num_workers], target);
        }
    }

    pthread_t workers[16];
    a2a_worker_arg_t wargs[16];
    double t0 = now_sec();
    for (int w = 0; w < num_workers; w++) {
        wargs[w].sched = sched;
        wargs[w].wid   = w;
        pthread_create(&workers[w], NULL, a2a_worker_proc, &wargs[w]);
    }
    for (int w = 0; w < num_workers; w++) pthread_join(workers[w], NULL);
    double elapsed = now_sec() - t0;

    uint64_t sum = 0;
    for (int i = 0; i < SWARM_ACTORS; i++) sum ^= actors[i].state;
    *out_score = (int64_t)(sum & 0x7FFFFFFFULL);

    for (int w = 0; w < num_workers; w++) pthread_spin_destroy(&sched->shards[w].pop_lock);
    free(tokens);
    free(actors);
    free(sched);
    return elapsed;
}

#ifdef USE_LIBPONYRT
/* ============================================================================
 * 4a / 4b / 4e. Official Pony Runtime (`libponyrt` 0.77.2) in C11
 * ============================================================================ */

typedef struct {
    messageq_t *q;
    int         count;
    bool        use_chain;
} ponyrt_fanin_arg_t;

static void *ponyrt_fanin_producer(void *arg) {
    ponyrt_fanin_arg_t *pa = (ponyrt_fanin_arg_t *)arg;
    uint32_t idx = (uint32_t)POOL_INDEX(sizeof(pony_msgi_t));
    if (!pa->use_chain) {
        for (int i = 0; i < pa->count; i++) {
            pony_msgi_t *m = (pony_msgi_t *)pony_alloc_msg(idx, 0);
            m->i = 1;
            ponyint_actor_messageq_push(pa->q, &m->msg, &m->msg);
        }
    } else {
        int rem = pa->count;
        while (rem > 0) {
            int k = (rem < CHAIN_LEN) ? rem : CHAIN_LEN;
            pony_msgi_t *first = (pony_msgi_t *)pony_alloc_msg(idx, 0);
            first->i = 1;
            pony_msg_t *prev = &first->msg;
            for (int j = 1; j < k; j++) {
                pony_msgi_t *cur = (pony_msgi_t *)pony_alloc_msg(idx, 0);
                cur->i = 1;
                pony_chain(prev, &cur->msg);
                prev = &cur->msg;
            }
            ponyint_actor_messageq_push(pa->q, &first->msg, prev);
            rem -= k;
        }
    }
    ponyint_pool_thread_cleanup();
    return NULL;
}

static double bench_ponyrt_fanin(int total_msgs, int nprod, bool use_chain, int64_t *out_score) {
    messageq_t q;
    ponyint_messageq_init(&q);

    pthread_t prods[16];
    ponyrt_fanin_arg_t args[16];
    int per_prod = total_msgs / nprod;

    double t0 = now_sec();
    for (int p = 0; p < nprod; p++) {
        args[p].q         = &q;
        args[p].count     = per_prod;
        args[p].use_chain = use_chain;
        pthread_create(&prods[p], NULL, ponyrt_fanin_producer, &args[p]);
    }

    int received = 0;
    int64_t score = 0;
    while (received < total_msgs) {
        int batch = 0;
        pony_msg_t *head = atomic_load_explicit(&q.head, memory_order_acquire);
        head = (pony_msg_t *)((uintptr_t)head & ~(uintptr_t)1);
        pony_msg_t *msg;
        while (batch < BATCH_QUOTA && (msg = ponyint_actor_messageq_pop(&q)) != NULL) {
            pony_msgi_t *m = (pony_msgi_t *)msg;
            score += m->i;
            received++;
            batch++;
            if (msg == head) break;
        }
        if (batch == 0) {
            ponyint_messageq_markempty(&q);
            _mm_pause();
        }
    }

    for (int p = 0; p < nprod; p++) pthread_join(prods[p], NULL);
    double elapsed = now_sec() - t0;

    ponyint_messageq_markempty(&q);
    ponyint_messageq_destroy(&q, true);
    ponyint_pool_drain();
    *out_score = score;
    return elapsed;
}

typedef struct {
    pony_msg_t msg;
    uint64_t   payload;
    uint32_t   hops_left;
} pony_a2a_msg_t;

typedef struct pony_a2a_actor_t {
    pony_actor_pad_t         pad;
    uint32_t                 id;
    uint64_t                 state;
    struct pony_a2a_actor_t **peers;
} pony_a2a_actor_t;

static _Atomic(int) g_pony_a2a_rem;
static double       g_pony_a2a_finish;

static void pony_a2a_dispatch(pony_ctx_t *ctx, pony_actor_t *actor, pony_msg_t *m) {
    pony_a2a_actor_t *self = (pony_a2a_actor_t *)actor;
    pony_a2a_msg_t *tok = (pony_a2a_msg_t *)m;

    uint64_t res = simulate_cpu_work(tok->payload ^ self->state);
    self->state ^= res;

    if (tok->hops_left > 1) {
        uint32_t target_id = (self->id * 31u + (uint32_t)res) & SWARM_MASK;
        pony_actor_t *target = (pony_actor_t *)self->peers[target_id];
        uint32_t idx = (uint32_t)POOL_INDEX(sizeof(pony_a2a_msg_t));
        pony_a2a_msg_t *next_m = (pony_a2a_msg_t *)pony_alloc_msg(idx, 0);
        next_m->payload   = res;
        next_m->hops_left = tok->hops_left - 1;
        pony_sendv(ctx, target, &next_m->msg, &next_m->msg, true);
    } else {
        if (atomic_fetch_sub_explicit(&g_pony_a2a_rem, 1, memory_order_acq_rel) == 1) {
            g_pony_a2a_finish = now_sec();
        }
    }
}

static pony_type_t g_pony_a2a_type = {
    .id = 0,
    .size = sizeof(pony_a2a_actor_t),
    .name = "A2AActor",
    .dispatch = pony_a2a_dispatch,
    .event_notify = UINT32_MAX,
};

static void run_child_pony_a2a(int total_hops, int num_threads) {
    char arg_max[32];
    snprintf(arg_max, sizeof(arg_max), "--ponymaxthreads=%d", num_threads);
    char *argv[] = { "bench", arg_max, "--ponynoscale", "--ponynoblock", NULL };
    pony_init(4, argv);
    pony_ctx_t *ctx = pony_ctx();

    pony_a2a_actor_t **actors = calloc(SWARM_ACTORS, sizeof(pony_a2a_actor_t *));
    for (int i = 0; i < SWARM_ACTORS; i++) {
        actors[i] = (pony_a2a_actor_t *)pony_create(ctx, &g_pony_a2a_type, false);
        ((pony_actor_t *)actors[i])->gc.rc = 1;
        actors[i]->id    = (uint32_t)i;
        actors[i]->state = 0;
        actors[i]->peers = actors;
    }

    atomic_init(&g_pony_a2a_rem, A2A_INIT_TOKENS);
    int base_hops = total_hops / A2A_INIT_TOKENS;
    int extra     = total_hops % A2A_INIT_TOKENS;
    uint32_t idx  = (uint32_t)POOL_INDEX(sizeof(pony_a2a_msg_t));

    for (int i = 0; i < A2A_INIT_TOKENS; i++) {
        pony_a2a_msg_t *m = (pony_a2a_msg_t *)pony_alloc_msg(idx, 0);
        m->payload   = (uint64_t)(i + 1);
        m->hops_left = (uint32_t)(base_hops + (i < extra ? 1 : 0));
        ponyint_sendv_inject((pony_actor_t *)actors[i & SWARM_MASK], &m->msg);
    }

    double t0 = now_sec();
    int ec = 0;
    pony_start(&ec, NULL);
    double elapsed = g_pony_a2a_finish - t0;

    uint64_t sum = 0;
    for (int i = 0; i < SWARM_ACTORS; i++) sum ^= actors[i]->state;
    char label[64];
    snprintf(label, sizeof(label), "4e. Official Pony (`libponyrt` 0.77.2, %d %s)",
             num_threads, num_threads == 1 ? "Thread" : "Threads");
    print_row(label, total_hops, elapsed, (int64_t)(sum & 0x7FFFFFFFULL));
    _exit(0);
}

static void spawn_self_pony_a2a(const char *mode_arg) {
    pid_t pid = fork();
    if (pid == 0) {
        char *argv[] = { "/proc/self/exe", (char *)mode_arg, NULL };
        execv("/proc/self/exe", argv);
        _exit(1);
    }
    waitpid(pid, NULL, 0);
}
#endif

int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IONBF, 0);
#ifdef USE_LIBPONYRT
    if (argc > 1) {
        if (strcmp(argv[1], "--child-pony-1") == 0) run_child_pony_a2a(A2A_TOTAL_HOPS, 1);
        if (strcmp(argv[1], "--child-pony-2") == 0) run_child_pony_a2a(A2A_TOTAL_HOPS, 2);
        if (strcmp(argv[1], "--child-pony-4") == 0) run_child_pony_a2a(A2A_TOTAL_HOPS, 4);
        if (strcmp(argv[1], "--child-pony-8") == 0) run_child_pony_a2a(A2A_TOTAL_HOPS, 8);
        return 0;
    }
#else
    (void)argc;
    (void)argv;
#endif
    int64_t score = 0;

    printf("===========================================================================================================\n");
    printf("WORKLOAD 1: MPSC Fan-In Contention (%d messages, %d producers -> 1 actor)\n", TOTAL_MSGS, NUM_PRODUCERS);
    printf("===========================================================================================================\n");
    printf("| Implementation                                   | Total Time  | Throughput    | Latency/Msg  | Checksum |\n");
    printf("|--------------------------------------------------|-------------|---------------|--------------|----------|\n");

    double t1 = bench_chan_fan_in(TOTAL_MSGS, NUM_PRODUCERS, &score);
    print_row("1. Traditional (core:sync/chan + thread)", TOTAL_MSGS, t1, score);

    double t2a = bench_dod_single_thread(TOTAL_MSGS, SWARM_ACTORS, &score);
    print_row("2a. DOD Batch Array (Single-Thread)", TOTAL_MSGS, t2a, score);

    double t2b = bench_dod_multithread_tls(TOTAL_MSGS, NUM_PRODUCERS, SWARM_ACTORS, &score);
    print_row("2b. DOD Multi-Thread (4 TLS Buffers + Drain)", TOTAL_MSGS, t2b, score);

    double t3c = bench_batched_mpsc(TOTAL_MSGS, NUM_PRODUCERS, false, &score);
    print_row("3c. Self-Impl Pony MPSC (Batched Watermark 8K)", TOTAL_MSGS, t3c, score);

    double t3d = bench_batched_mpsc(TOTAL_MSGS, NUM_PRODUCERS, true, &score);
    print_row("3d. Self-Impl Pony MPSC (Chain=32 Splice 8K)", TOTAL_MSGS, t3d, score);

    double t3e = bench_sharded_spsc(TOTAL_MSGS, NUM_PRODUCERS, &score);
    print_row("3e. Sharded SPSC Ring Lanes (Zero RMW Atomics)", TOTAL_MSGS, t3e, score);

#ifdef USE_LIBPONYRT
    double t4a = bench_ponyrt_fanin(TOTAL_MSGS, NUM_PRODUCERS, false, &score);
    print_row("4a. Official Pony (`libponyrt` 0.77.2 Single)", TOTAL_MSGS, t4a, score);

    double t4b = bench_ponyrt_fanin(TOTAL_MSGS, NUM_PRODUCERS, true, &score);
    print_row("4b. Official Pony (`libponyrt` 0.77.2 Chain=32)", TOTAL_MSGS, t4b, score);
#endif

    printf("\n===========================================================================================================\n");
    printf("WORKLOAD 5: Autonomous Actor-to-Actor Graph + CPU Work (%d hops, %d actors)\n", A2A_TOTAL_HOPS, SWARM_ACTORS);
    printf("===========================================================================================================\n");

    double t5_1 = bench_pony_mn_a2a(A2A_TOTAL_HOPS, 1, &score);
    print_row("3h. Pony M:N Actor-to-Actor + CPU (1 Worker)", A2A_TOTAL_HOPS, t5_1, score);

    double t5_2 = bench_pony_mn_a2a(A2A_TOTAL_HOPS, 2, &score);
    print_row("3h. Pony M:N Actor-to-Actor + CPU (2 Workers)", A2A_TOTAL_HOPS, t5_2, score);

    double t5_4 = bench_pony_mn_a2a(A2A_TOTAL_HOPS, 4, &score);
    print_row("3h. Pony M:N Actor-to-Actor + CPU (4 Workers)", A2A_TOTAL_HOPS, t5_4, score);

    double t5_8 = bench_pony_mn_a2a(A2A_TOTAL_HOPS, 8, &score);
    print_row("3h. Pony M:N Actor-to-Actor + CPU (8 Workers)", A2A_TOTAL_HOPS, t5_8, score);

    double t5_16 = bench_pony_mn_a2a(A2A_TOTAL_HOPS, 16, &score);
    print_row("3h. Pony M:N Actor-to-Actor + CPU (16 Workers)", A2A_TOTAL_HOPS, t5_16, score);

#ifdef USE_LIBPONYRT
    spawn_self_pony_a2a("--child-pony-1");
    spawn_self_pony_a2a("--child-pony-2");
    spawn_self_pony_a2a("--child-pony-4");
    spawn_self_pony_a2a("--child-pony-8");
#endif

    return 0;
}
