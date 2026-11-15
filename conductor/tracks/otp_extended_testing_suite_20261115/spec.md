# Track Specification: OTP ATDD Test Harness, Scenario DSL & Reference Spec Suite

**Track ID:** `otp_extended_testing_suite_20261115`
**Prerequisite Track:**
- [`runtime_codegen_hardening_20261115`](../runtime_codegen_hardening_20261115/spec.md) (Track 1 — provides worker `init_context` `Tracking_Allocator` inheritance, `.Seq_Cst` Dekker `wait_idle`, offset-0 `Call_Slot` `Futex`, and `actor.forward` call-delegation flag preservation)

**Downstream Implementation Tracks (Turn These Specs Green):**
- [`otp_supervision_20261115`](../otp_supervision_20261115/spec.md) (Track 3 — turns `spec_01`–`spec_16`, `spec_19`–`spec_26`, `spec_28`, and `prop_18` Green on the first pass)
- [`otp_multitenant_and_codegen_20261115`](../otp_multitenant_and_codegen_20261115/spec.md) (Track 4 — turns `spec_17`, `spec_27`, `test_04`–`test_06`, and `prop_29` Green)

**Primary References (`.tmp/reference_repos/`):**
- Erlang/OTP: `otp/lib/stdlib/test/supervisor_SUITE.erl` (`4,052` lines)
- Elixir: `elixir/lib/elixir/test/elixir/{supervisor,dynamic_supervisor,partition_supervisor,registry}_test.exs` (`1,643` lines)
- C++ Actor Framework (CAF): `actor-framework/libcaf_core/caf/{actor_termination,typed_response_promise,actor_registry}.test.cpp` (`468` lines)
- Rust `ractor`: `ractor/ractor/src/{actor/supervision_tests.rs,thread_local/supervision_tests.rs,rpc/tests.rs,registry/tests.rs}` & `ractor/ractor/tests/ui/actor_macros/*.rs`
- Rust `ractor-supervisor`: `ractor-supervisor/src/{supervisor,dynamic}.rs` (`2,049` lines)

---

## 1. Upstream Reference Test Suite Audit Matrix & Red-First ATDD Strategy

Authoring Sleipnir's full 4-layer ATDD test infrastructure and complete specification suite **before** implementing the runtime `Supervisor` state machine (`Track 3`) and multi-tenant / codegen subsystems (`Track 4`) guarantees the runtime is engineered once against all edge cases (`spec_01`–`spec_28`, `prop_18`, `prop_29`, `test_04`–`test_06`) without intermediate rewrites.

Auditing the upstream unit, concurrency, and UI test suites across all 5 cloned reference repositories yields both the core OTP specifications (`spec_01`–`spec_17`, `prop_18`, `test_04`) and **12 high-value edge cases and invariants** (`spec_19`–`spec_28`, `prop_29`, `test_05`–`test_06`):

| Upstream Repo & Test File | Upstream Test Cases / Lines | Coverage Status & Sleipnir Target |
| :--- | :--- | :--- |
| **Erlang/OTP** (`supervisor_SUITE.erl`) | `one_for_one*`, `one_for_all*`, `rest_for_one*`, `simple_one_for_one*` | **`spec_01`–`spec_09`, `spec_16`, `prop_18`** (Green in Track 3) |
| **CAF / Elixir** (`actor_termination`, `partition_supervisor_test.exs`) | Typed monitors, `try_call` cancellation, `vmem.Arena` & multi-tenant sharding | **`spec_10`–`spec_15`**, **`spec_17`** (Green in Tracks 3 & 4) |
| **Erlang/OTP** (`supervisor_SUITE.erl`) | `one_for_all_other_child_fails_restart` (`L1546–1592`), `rest_for_one_other_child_fails_restart` (`L1849–1889`), `hanging_restart_loop*` (`L2411–2558`) | **`spec_21`**: Cascading sibling `on_reinit` failure & rollback (Green in Track 3) |
| **Erlang/OTP** (`supervisor_SUITE.erl`) & **Elixir** (`dynamic_supervisor_test.exs`) | `temporary_bystander` (`L1380–1404`), `simple_one_for_one_corruption` (`OTP-16804`, `L1713–1762`), `do_not_save_start_parameters_for_temporary_children` (`L2127–2275`), `dynamic_supervisor_test.exs` (`L281–292`) | **`spec_22`**: `.Temporary` bystander non-restart & zero spec/arena retention (Green in Tracks 3 & 4) |
| **Erlang/OTP** (`supervisor_SUITE.erl`) | `sup_stop_race` (`L573–630`), `sup_stop_non_shutdown_exit_dynamic` (`L634–658`), `child_unlink` (`L1894–1916`) | **`spec_23`**: Supervisor shutdown vs concurrent child crash race (Green in Track 3) |
| **Erlang/OTP** (`supervisor_SUITE.erl`) & **Ractor** (`supervision_tests.rs`) | `order_of_children` (`L2741–2824`), `tree` (`L1919–2006`), `scale_start_stop_many_children` (`L2828–2896`), `simple_one_for_one_scale_many_temporary_children` (`OTP-9242`), `terminating_a_deep_supervision_tree_is_iterative` (`L1163–1208`) | **`spec_24`**: Non-alphabetical start/stop ordering, deep iterative teardown & 256-child scaling (Green in Track 3) |
| **Erlang/OTP** (`supervisor_SUITE.erl`), **Elixir** (`supervisor_test.exs`, `dynamic_supervisor_test.exs`, `partition_supervisor_test.exs`), **ractor-supervisor** (`dynamic.rs`) | `child_adm` (`L813–882`), `child life cycle` (`L206–287`), `count_children` (`L2009–2113`, `L260–307`), `which_children` (`L3838–3901`), `max_children` (`dynamic_supervisor_test.exs:274–279`, `dynamic.rs:590–640`) | **`spec_25`**: Dynamic child administration (`terminate_child`, `restart_child`, `delete_child`, `count_children`) & `max_children` backpressure (Green in Tracks 3 & 4) |
| **ractor-supervisor** (`supervisor.rs`, `dynamic.rs`) | `test_restart_counter_reset_after` (`L984–1068`), `test_child_level_restart_counter_reset_after` (`L1070–1165`), `test_dynamic_supervisor_reset_after` (`L750–800`) | **`spec_26`**: Quiet-period intensity reset & multi-burst survival (Green in Track 3) |
| **CAF** (`actor_termination.test.cpp`, `actor_registry.test.cpp`), **Elixir** (`registry_test.exs`), **Ractor** (`registry/tests.rs`), **OTP** (`simple_global_supervisor`) | `test_duplicate_registration` (`L57–111`), `test_actor_registry_unenrollment` (`L118–154`), `clean up registry on process crash` (`L58–85`), `actors clean up incoming edges when they terminate` (`L110–200`) | **`spec_27`**: Registry duplicate guard, auto-unregister on permanent stop & monitor edge cleanup (Green in Tracks 3 & 4) |
| **CAF** (`typed_response_promise.test.cpp`) & **Ractor** (`rpc/tests.rs`) | `response promises allow delegation` (`L109–124`), `response promises send errors when broken` (`L126–162`), `test_rpc_call_forwarding` (`L275–345`) | **`spec_19`**: `actor.forward` call delegation & broken-promise propagation (Green in Track 3) |
| **Ractor** (`rpc/tests.rs`) & **CAF** (`actor_termination.test.cpp`) | `test_rpc_call` (`L178`), `test_multi_call` (`L347–444`), `multiple multiplexed requests` (`L61–108`) | **`spec_28`**: `try_call` to already-`.Stopped` / stale-generation target fast-fail & multi-call fan-out (Green in Track 3) |
| **Ractor** (`actor/supervision_tests.rs`) | `test_supervision_panic_in_post_stop` (`L571–649`), `test_supervision_error_in_post_stop` (`L651–730`), `stopping_children_and_wait_during_parent_shutdown` (`L1384–1445`) | **`spec_20`**: Fault-contained `on_stop` hook during restart & shutdown (Green in Track 3) |
| **Ractor** (`tests/ui/actor_macros/*.rs`) | `mixed_supervision_dispatch.rs`, `rpc_ambiguous_reply.rs`, `rpc_missing_reply.rs` | **`test_04`, `test_05`, `test_06`**: Positive `@(supervise)`/`@(reinit)` emission & negative AST codegen diagnostics (Green in Track 4) |

*(Note: OTP `significant_*` tests [`L3144–3810`] and Elixir `Supervisor` shutdown grace periods represent optional future extensions; Sleipnir tracks child teardown synchronously via `on_stop` hooks in `spec_01`–`spec_28`.)*

### 1.1 4-Layer ATDD Test Architecture & Compile-Ready Layer 4 Contract Skeleton

Track 2 constructs all 4 ATDD layers in a strict compile-ready Red state:
1. **Layer 4 Contract Skeleton (`actor/supervision.odin`, `actor/doc.odin`, `actor/codegen/codegen.odin`):**
   - Declare all public supervision, fault-control, monitor, `try_call`, `Registry($Key)`, `Partitioned_Supervisor` / `Dynamic_Supervisor`, and codegen diagnostic types/procedures with compile-ready stub bodies that return deterministic placeholder values (never hanging, never blocking on unfulfilled `Call_Slot`s, and never leaking allocations under `with_leak_check`).
   - Update `ols.json` (`"checker_args": "-custom-attribute:actor,behavior,no_behavior,supervise,reinit"`) so `odin check` and `odin test` accept `@(supervise)` and `@(reinit)` across `./actor` and `./tests`.
   - Core contract exports include:
     - Fault & Lifecycle: `Actor_Status`, `Fault_Kind`, `Fault_Info`, `Exit_Reason`, `Call_Error`, `exit_turn`, `fail_turn`, `defer_on_fault`, `current_generation`, `Allocator`.
     - Monitors & RPC: `Monitor_Ref`, `Down_Event`, `monitor`, `demonitor`, `try_call`.
     - Supervisor & Child Admin: `Restart_Strategy`, `Restart_Policy`, `Clock_Proc`, `Supervisor_Config`, `Supervisor_Msg`, `Supervisor`, `Supervisor_Error`, `Child_Counts`, `Child_Info`, `spawn_supervisor`, `supervise`, `supervise_supervisor`, `supervisor_terminate_child`, `supervisor_restart_child`, `supervisor_delete_child`, `supervisor_count_children`, `supervisor_which_children`.
     - Registry & Multi-Tenant: `Registry`, `registry_init`, `registry_destroy`, `registry_register`, `registry_unregister`, `registry_lookup`, `register_name`, `unregister_name`, `whereis_name`, `Partitioned_Supervisor_Config`, `Partitioned_Supervisor`, `Dynamic_Supervisor`, `partitioned_supervisor_init`, `partitioned_supervisor_destroy`, `get_or_spawn_tenant`, `send_to_tenant`, `try_call_tenant`, `call_tenant`, `sweep_idle_tenants`, `tenant_count`, and `dynamic_supervisor_*` wrappers.
     - Codegen Validation: `Diagnostic`, `validate_module_spec(spec: ^Module_Spec) -> ([]Diagnostic, bool)`.
2. **Layer 3 Protocol Driver (`tests/supervision_driver_test.odin`):**
   - Wraps `actor.Scheduler` (supporting `num_workers = 1` cooperative mode and `num_workers >= 2` multi-threaded mode), `Virtual_Clock` (`virtual_clock_tick`, `advance_ms`), deterministic `settle()`, `Fault_Injector`, `Watcher_Probe`, lifecycle trace recorders (`record_stop_order`, `record_reinit_order`), deep nested supervisor builders, router/worker delegation fixtures, and multi-tenant sweep harnesses.
3. **Layer 2 Scenario DSL (`tests/supervision_dsl_test.odin`):**
   - Exposes fluent `given_*`, `when_*`, `then_*` helpers capturing `loc := #caller_location` around `testing.expect*` assertions so failing Red specs report exact Layer 1 line numbers.
4. **Layer 1 Executable Specifications (`tests/supervision_spec_test.odin`, `tests/codegen_test.odin`):**
   - Authors `spec_01`–`spec_16` + `prop_18` (specified in detail in [Track 3 `spec.md`](../otp_supervision_20261115/spec.md)), `spec_17` + `test_04` (specified in detail in [Track 4 `spec.md`](../otp_multitenant_and_codegen_20261115/spec.md)), and `spec_19`–`spec_28`, `prop_29`, `test_05`–`test_06` (specified in Section 2 below), all wrapped in `with_leak_check`.

---

## 2. Detailed Functional & Runtime Requirements (`spec_19`–`spec_28`, `test_05`–`test_06`, `prop_29`)

*(Note: Core specs `spec_01`–`spec_16` + `prop_18` are detailed in [Track 3 `spec.md`](../otp_supervision_20261115/spec.md), and `spec_17` + `test_04` are detailed in [Track 4 `spec.md`](../otp_multitenant_and_codegen_20261115/spec.md).)*

### 2.1 Synchronous Call Delegation via `actor.forward` (`spec_19`)

- **Runtime Requirement (`actor/actor.odin` — Track 1 + Track 3):**
  - When a forwarding actor (`router`) receives a `call` / `try_call` envelope (`ENVELOPE_FLAG_CALL`, non-nil `env.reply_slot`) and calls `actor.forward(worker, msg)`:
    1. `forward_ctx` / `forward_sched` clears `ctx.current_reply_slot = nil` and `ctx.current_flags &~= (ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID)` on the **forwarding actor's stack `Context`**, so `router` ending its turn (or calling `reply` after `forward`) does not prematurely complete or cancel the caller's `Call_Slot`.
    2. `forward_*` **preserves `env.reply_slot` and `env.flags` (`ENVELOPE_FLAG_CALL | ENVELOPE_FLAG_REPLY_PID`) on the forwarded `Envelope`**.
    3. If `worker` calls `actor.reply(val)` (matching `Res`), it completes the original caller's `Call_Slot` directly (`(val, .None)`).
    4. If `worker` panics, calls `fail_turn`, or stops before calling `reply`, `worker`'s fault handler completes the original caller's `Call_Slot` with `({}, .Call_Target_Crashed)` or `({}, .Target_Stopped)`.
- **`spec_19_delegated_call_via_forward_and_broken_promise`:**
  - **19a (Delegation Happy Path):** Caller calls `try_call(&sched, router, Request{x = 21}, int)`; `router` executes `actor.forward(worker, msg)`; `worker` replies `42`; caller receives `(42, .None)` while `router` exits its turn cleanly without spurious call-missing faults.
  - **19b (Broken Delegated Promise):** `router` forwards `Request` to `worker`, and `worker` panics (`panic("worker crash")`) or exits without replying; caller receives `(0, .Call_Target_Crashed)` (zero hang, zero leaked `Call_Slot`), and `worker`'s supervisor restarts `worker` cleanly.

---

### 2.2 Fault-Contained `on_stop` Hooks (`spec_20`)

- **Runtime Requirement (`actor/supervision.odin` — Track 3):**
  - Wrap every `child.on_stop` (`child_stop_proc`) invocation inside a `Trap_Frame` (during both `supervisor_terminate_range` in strategy restarts and `stop_supervisor` / `scheduler_stop_and_destroy` teardown).
  - If `on_stop` raises an Odin panic or calls `actor.fail_turn(...)`:
    1. The trap catches the fault, logs a warning/telemetry event (`Supervision_Event.Child_Stop_Fault`), and does **not** unwind out of the supervisor's turn.
    2. The child is still transitioned to `.Stopped`, its arena rewound/destroyed safely, and stopping/restarting continues uninterrupted across remaining siblings and children.
- **`spec_20_fault_contained_on_stop_hook_during_restart_and_shutdown`:**
  - `[C1, C2, C3]` under `.Rest_For_One`; `C2` installs an `on_stop` hook that panics once (and in a sibling subcase calls `fail_turn`); `C1` crashes -> `C3.on_stop` runs, `C2.on_stop` panics and is trapped without aborting the supervisor, `[C1, C2, C3]` all `on_reinit` in forward order and process subsequent messages.
  - Second assertion: shutting down a supervisor whose child's `on_stop` permanently panics finishes cleanly with all sibling `on_stop` hooks invoked and `0 B` leaked.

---

### 2.3 Cascading Sibling `on_reinit` Failure & Batch Rollback (`spec_21`)

- **Runtime Requirement (`actor/supervision.odin` — Track 3):**
  - When a `.One_For_All` or `.Rest_For_One` restart pass re-initializes `[C_k, C_{k+1}, ...]` in forward order after stopping `[..., C_{k+1}, C_k]` in reverse order:
    - If `C_k.on_reinit` succeeds (`C_k` is now `.Running`), and a younger sibling `C_{k+1}.on_reinit` fails (`false` / panics / `fail_turn`):
      1. The supervisor must immediately stop the newly restarted `C_k` (running `C_k.on_stop` under `Trap_Frame`, rewinding its arena to `.Stopped`) so no half-initialized cohort remains `.Running`.
      2. The failed sibling `on_reinit` records a restart attempt against the supervisor's `Restart_Window`.
      3. If `restart_count <= max_restarts`, the supervisor retries starting the cohort (`[C_k, C_{k+1}, ...]`); if `max_restarts` is exceeded, the supervisor shuts down all children cleanly and escalates.
- **`spec_21_cascading_sibling_on_reinit_failure_rolls_back_restarted_peers`:**
  - `[C1, C2, C3]` under `.Rest_For_One` (`max_restarts = 3`); `C1` crashes, `C1.on_reinit` succeeds (gen `1`), `C2.on_reinit` fails on attempt `1` and succeeds on attempt `2` -> verify `C1` was rolled back (`on_stop` count incremented) before the retry so `C1` ends at gen `2`, `C2` at gen `1` (or `2`), `C3` at gen `1`, and verify escalation when `C2.on_reinit` permanently fails past `max_restarts`.

---

### 2.4 `.Temporary` Bystander Non-Restart & Memory Hygiene (`spec_22`)

- **Runtime Requirement (`actor/supervision.odin` — Tracks 3 & 4):**
  - When `.One_For_All` or `.Rest_For_One` stops a cohort in reverse order, any bystander with `restart == .Temporary` has its `on_stop` executed (under `Trap_Frame`), transitions permanently to `.Stopped`, is **skipped** during the forward `on_reinit` pass, and releases its `Reset_Desc` / child arena without incrementing the `Restart_Window` failure count for bystander termination.
  - Under `Partitioned_Supervisor` / `Dynamic_Supervisor`, `.Temporary` workers that exit cleanly or crash release their `Child_Spec` and `Reset_Desc` memory immediately upon teardown.
- **`spec_22_temporary_bystander_stopped_not_restarted_and_specs_reclaimed`:**
  - `[C1 (.Permanent), C2 (.Temporary), C3 (.Permanent)]` under `.Rest_For_One`: crashing `C1` stops `[C3, C2]`, restarts `[C1, C3]` only, leaves `C2.status == .Stopped`, and `which_children` / `count_children` excludes restarted `C2`.
  - Spawning and crashing `500` `.Temporary` children under `Partitioned_Supervisor` retains `0` dead `Tenant_Entry` / `Reset_Desc` allocations and passes `with_leak_check` with `0 B` leaked.

---

### 2.5 Supervisor Shutdown vs Concurrent Child Crash Race (`spec_23`)

- **Runtime Requirement (`actor/supervision.odin` — Track 3):**
  - Guard supervisor restart/event processing on an atomic lifecycle state (`SUP_RUNNING`, `SUP_STOPPING`, `SUP_STOPPED`). If a worker thread traps a child panic while the supervisor concurrently transitions to `SUP_STOPPING` (`stop_supervisor` / `scheduler_stop_and_destroy`), `on_child_crashed` / `Supervisor_Event` handling must observe `SUP_STOPPING` and decline `on_reinit` without UAF or deadlock.
- **`spec_23_supervisor_shutdown_concurrent_child_crash_race`:**
  - Run across `4` OS worker threads (repeated `20` iterations): `8` `.Permanent` child actors flood themselves with self-messages that trigger panics while the test driver concurrently invokes `stop_supervisor` / `scheduler_stop_and_destroy`. Assert zero hangs, zero restarted stragglers after stop, zero panics on freed memory, and `0 B` leaked under `-o:speed` and debug builds.

---

### 2.6 Non-Alphabetical Start/Stop Order, Deep Iterative Teardown & High Child-Count Scaling (`spec_24`)

- **Runtime Requirement (`actor/supervision.odin` — Track 3):**
  - Maintain explicit insertion order in `sup.children` (slice/array, never map iteration order), guaranteeing children start `[0 ..< len)` and stop `[len-1 ..>= 0]`.
  - Implement supervisor subtree teardown (`stop_supervisor`) iteratively using an explicit work stack/queue (no recursive Odin stack frames proportional to tree depth) so arbitrarily deep hierarchies (`16+` nested supervisors) tear down cleanly on small worker stacks.
- **`spec_24_non_alphabetical_child_order_and_deep_tree_iterative_teardown`:**
  - **24a (`order_of_children`):** Register `[ch7, ch3, ch10, ch2, ch5]` under `.Rest_For_One`. Crashing `ch10` asserts exact stop trace `["ch5", "ch2"]` and reinit trace `["ch10", "ch2", "ch5"]`; stopping the supervisor asserts exact stop trace `["ch5", "ch2", "ch10", "ch3", "ch7"]`.
  - **24b (`deep_tree` + `scale_start_stop_many_children`):** Build a `16`-level chain of nested supervisors plus a flat supervisor with `256` children, route a message end-to-end, and tear down cleanly with `0` stack overflow and `0 B` leaked.

---

### 2.7 Dynamic Child Administration & `max_children` Backpressure (`spec_25`)

- **Runtime Requirement (`actor/supervision.odin` & `actor/doc.odin` — Tracks 3 & 4):**
  - Define explicit administrative APIs and error enum:
    ```odin
    Supervisor_Error :: enum {
        None,
        Not_Found,
        Child_Still_Running,
        Child_Already_Present,
        Max_Children_Reached,
        Supervisor_Stopping,
    }

    Child_Counts :: struct {
        specs:       int, // total retained child entries (including manually terminated non-temporary specs)
        active:      int, // currently .Running children
        supervisors: int,
        workers:     int,
    }
    ```
  - Procedures:
    - `supervisor_terminate_child(sup: ^Supervisor, child_id: u64) -> Supervisor_Error` — Stops a running child (`on_stop`) and marks its entry `.Terminated_By_Admin` (does **not** auto-restart even if `.Permanent`, and removes `.Temporary` specs after stop).
    - `supervisor_restart_child(sup: ^Supervisor, child_id: u64) -> Supervisor_Error` — Re-initializes a `.Stopped` / `.Terminated_By_Admin` spec via `on_reinit` (increments `generation`); returns `.Child_Still_Running` if already `.Running`.
    - `supervisor_delete_child(sup: ^Supervisor, child_id: u64) -> Supervisor_Error` — Removes a `.Stopped` / `.Terminated_By_Admin` spec; returns `.Child_Still_Running` if `.Running`.
    - `supervisor_count_children(sup: ^Supervisor) -> Child_Counts` & `supervisor_which_children(sup: ^Supervisor, allocator := context.allocator) -> []Child_Info`.
    - Support `max_children: int` (`<= 0` means unbounded) in `Supervisor_Config` and `Partitioned_Supervisor_Config`, returning `.Max_Children_Reached` when `active >= max_children`.
- **`spec_25_dynamic_child_administration_and_max_children_backpressure`:**
  - Verify the full child lifecycle (`start -> count -> terminate_child [verify no auto-restart for .Permanent] -> delete_child fails while running [when restarted] -> stop -> delete_child -> count == 0`) and `max_children = 3` rejection on the 4th spawn (plus recovery once one child stops/is deleted).

---

### 2.8 Quiet-Period Intensity Reset & Multi-Burst Survival (`spec_26`)

- **Runtime Requirement (`actor/supervision.odin` — Track 3):**
  - Confirm `Restart_Window` timestamps are stamped using `sched.clock_fn()` (`Virtual_Clock` in tests, `time.tick_now()` in production) and prune expired entries strictly outside `window_duration` so bursts separated by quiet intervals do not accumulate into spurious escalation.
- **`spec_26_quiet_period_intensity_reset_survives_multi_burst_faults`:**
  - Configure `max_restarts = 2`, `window_ms = 100`. Inject `2` panics at `t = 0ms`, `20ms`; advance `Virtual_Clock` by `150ms`; inject `2` more panics at `t = 170ms`, `190ms`; advance `Virtual_Clock` by `150ms`; inject `2` more panics (`6` total restarts across `3` bursts with `0` escalation); then inject `3` panics within `50ms` and assert immediate supervisor escalation (`total_restarts` stops cleanly at meltdown).

---

### 2.9 Registry Duplicate Guard, Auto-Unenroll on Permanent Stop & Monitor Cleanup (`spec_27`)

- **Runtime Requirement (`actor/supervision.odin` — Tracks 3 & 4):**
  - `registry_register(reg, key, pid)` checks if `key` is already bound to a live `.Running` target; if so, returns `false` without overwriting the existing live owner (even if `pid` differs).
  - Actors track lightweight registry backlinks (or clean unenrollment hooks on permanent `.Stopped` transition inside `handle_actor_fault` / `actor_base_destroy` / `stop_supervisor`) so when an actor permanently stops (`.Normal` stop, `.Temporary` crash, `supervisor_terminate_child`, or supervisor meltdown), its `Registry($Key)` entry and incoming `Monitor_Node` edges are cleaned up automatically without waiting for `scheduler_destroy`.
  - When a supervised actor restarts in-place (`on_reinit`), its `Registry($Key)` entry either remains bound to the live incremented generation `PID` (if re-registered in `on_reinit`) or reflects the latest live `PID`.
- **`spec_27_registry_duplicate_guard_and_auto_unenroll_on_permanent_stop`:**
  - Verify: (a) duplicate `registry_register("svc_a", w2)` returns `false` while `w1` is `.Running`; (b) crashing `.Temporary` `w1` auto-unenrolls `"svc_a"` so `registry_lookup` returns `({}, false)` and `"svc_a"` can immediately be re-registered to `w2`; (c) crashing `w1` delivers `Down_Event` to all monitors and drains incoming monitor links cleanly with `0` leaked `Monitor_Node` entries.

---

### 2.10 `try_call` Dead-Target Fast-Fail & Fan-Out Multi-Call (`spec_28`)

- **Runtime Requirement (`actor/actor.odin` — Track 3):**
  - At the entry of `try_call` (both external and actor-context paths), check atomic `target.status` and `pid.generation`:
    - If `target.status == .Stopped`, return immediately with `({}, .Target_Stopped)` without allocating an envelope or parking on `Call_Slot`.
    - If `pid.generation != atomic_load_explicit(&target.generation, .Acquire)`, return immediately with `({}, .Stale_Generation)`.
- **`spec_28_try_call_dead_target_fast_fail_and_multi_call_fanout`:**
  - **28a (Dead & Stale Target Fast-Fail):** Calling `try_call` on an already `.Stopped` actor or stale gen-`0` `PID` after gen-`1` restart returns `.Target_Stopped` / `.Stale_Generation` immediately (`< 1ms`).
  - **28b (Fan-Out Multi-Call Batch):** Issue sequential/parallel `try_call` calls across `4` supervised workers where worker `#2` (`.Temporary`) panics mid-turn; assert workers `#0, #1, #3` return `(expected, .None)` and worker `#2` returns `(0, .Call_Target_Crashed)` cleanly with zero hangs.

---

### 2.11 Negative AST Codegen Validation Tests (`test_05` & `test_06`)

- **Target File:** `tests/codegen_test.odin` + `actor/codegen/codegen.odin` (Stub in Track 2; Green in Track 4)
- **Requirement:** Add `validate_module_spec(spec: ^Module_Spec) -> ([]Diagnostic, bool)` in `actor/codegen/codegen.odin` so invalid attributes/signatures are detected with file/line/message diagnostics before emitting Odin files:
  - **`test_05_codegen_rejects_invalid_supervise_and_reinit_attributes`:**
    - Rejects `@(supervise="strategy=unknown_mode")`, `@(supervise="max_restarts=-1")`, multiple `@(reinit)` procedures targeting the same `@(actor)` struct, `@(reinit)` with mismatched receiver pointer type, and a procedure annotated with both `@(behavior)` and `@(reinit)`.
  - **`test_06_codegen_warns_or_rejects_ambiguous_rpc_reply_signature`:**
    - Rejects a `@(behavior)` procedure declaring conflicting `reply_to` parameters (e.g., duplicate reply channels) or malformed `reply_to: actor.Reply($T)` syntax cleanly without generating broken code.

---

### 2.12 Multi-Threaded Fault-Injection & Linearizability Property (`prop_29`)

- **Target File:** `tests/supervision_spec_test.odin` (Red in Track 2; Green in Track 4)
- **`prop_29_multithreaded_fault_injection_supervision_linearizability`:**
  - Across `4` OS worker threads and `50` deterministic PRNG seeds, concurrently execute random operations (`send`, `try_call`, `actor.forward`, `monitor`, `registry_register` / `lookup`, `supervisor_terminate_child` / `restart_child`, `panic`, `fail_turn`, `.Normal` stop) across a 3-level supervision hierarchy.
  - Assert all 7 runtime invariants after settle and teardown on every seed:
    1. `active_actors == 0` on idle.
    2. Zero orphaned `.Running` descendants under any `.Stopped` supervisor.
    3. Strictly monotonic `generation` counters across every child restart.
    4. `100%` of live monitors receive exactly one `Down_Event` per permanent stop or restart transition as configured.
    5. Zero dangling `Registry($Key)` entries pointing to `.Stopped` actors.
    6. Zero blocked `try_call` waiters after actors crash or stop.
    7. `0 B` leaked and `0` bad frees in `Tracking_Allocator`.

---

## 3. Acceptance Criteria (Track 2 Completion Gate)

1. `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors` and `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors` pass with zero errors or warnings.
2. `ols.json` includes `"checker_args": "-custom-attribute:actor,behavior,no_behavior,supervise,reinit"`.
3. All existing Track 1 runtime and codegen tests (`tests/actor_test.odin`, `test_01`–`test_03` in `tests/codegen_test.odin`) continue to pass (`0` failures, `0 B` leaked).
4. All newly authored Red ATDD specifications (`spec_01`–`spec_28`, `prop_18`, `prop_29` in `tests/supervision_spec_test.odin` and `test_04`–`test_06` in `tests/codegen_test.odin`) compile cleanly and **fail deterministically (Red)** against the Layer 4 stubs with zero hangs, zero infinite spins, zero segfaults/crashes, and zero leaked test-harness memory—ready for [Track 3 (`otp_supervision_20261115`)](../otp_supervision_20261115/spec.md) and [Track 4 (`otp_multitenant_and_codegen_20261115`)](../otp_multitenant_and_codegen_20261115/spec.md) to turn Green on the first pass.
5. No files inside `conductor/tracks/otp_supervision_20261115/references/` are modified.
