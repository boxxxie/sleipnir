# Track Specification: Multi-Tenant Partitioned Supervision, `Registry($Key)` & `@(supervise)`/`@(reinit)` Codegen

**Track ID:** `otp_multitenant_and_codegen_20261115`
**Prerequisite Tracks:**
- [`runtime_codegen_hardening_20261115`](../runtime_codegen_hardening_20261115/spec.md) (Track 1)
- [`otp_extended_testing_suite_20261115`](../otp_extended_testing_suite_20261115/spec.md) (Track 2 — pre-authored Red ATDD specs `spec_17`, `spec_22`, `spec_25`, `spec_27`, `test_04`–`test_06`, `prop_29`)
- [`otp_supervision_20261115`](../otp_supervision_20261115/spec.md) (Track 3 — core OTP `Supervisor`, `Trap_Frame`, typed monitors, and `try_call`)

**Primary References:**
- [`OTP carbon review.md`](../otp_supervision_20261115/references/OTP%20carbon%20review.md)
- [`Porting OTP test scenarios.md`](../otp_supervision_20261115/references/Porting%20OTP%20test%20scenarios.md) (`Spec 17`)
- Reference Snippets `06_banking_reinit_example.odin`–`22_ci_workflow.yml`
- Upstream Elixir & Rust references: `.tmp/reference_repos/elixir/lib/elixir/lib/{partition_supervisor,dynamic_supervisor,registry}.ex`, `.tmp/reference_repos/ractor-supervisor/src/dynamic.rs`

---

## 1. Objective

Complete Sleipnir's OTP ecosystem for production multi-tenant services and declarative AST code generation, turning Track 2's remaining Red specifications (`spec_17`, `spec_22` multi-tenant reclamation, `spec_25` multi-tenant `max_children`, `spec_27`, `test_04`–`test_06`, and `prop_29`) 100% Green:
1. Safe live actor reclamation (`actor_base_destroy`) while the scheduler is running.
2. Thread-safe generic `Registry($Key)` (`O(1)` lookup under shared `sync.RW_Mutex`) with live-owner duplicate registration guard and automatic unenrollment on permanent actor stop (`spec_27`).
3. Elixir-inspired sharded multi-tenant `Partitioned_Supervisor` (with `Dynamic_Supervisor` compatibility alias/wrappers) featuring atomic `in_flight` call/send protection, `max_children` backpressure (`spec_25`), immediate `.Temporary` spec/arena reclamation (`spec_22`), and two-phase idle eviction (`spec_17`).
4. Declarative `@(supervise)` and `@(reinit)` AST attributes and `validate_module_spec` diagnostic checks in `tools/sleipnir_gen` (`actor/codegen/codegen.odin`) turning `test_04`–`test_06` Green.
5. Full multi-threaded fault-injection linearizability verification (`prop_29`), end-to-end 50-organization x 3-worker pipeline showcase (`examples/06_otp_supervision.odin`), and GitHub Actions CI enforcement (`.github/workflows/ci.yml` & `ols.json`).

---

## 2. Detailed Functional & Technical Requirements

### 2.1 Live Actor Reclamation (`actor_base_destroy`)

**Where:** `actor/actor.odin` / `actor/supervision.odin` (`references/15_actor_base_destroy.odin`, `OTP carbon review.md`)

When evicting an idle tenant subtree or reclaiming a `.Temporary` worker while worker threads continue running:
1. Mark the actor `.Stopped` (`Exit_Reason = .Supervisor_Shutdown`), unenroll any attached `Registry($Key)` backlinks, emit pending `Down_Event` notifications, and drain incoming `Monitor_Node` edges while preventing re-scheduling.
2. Spin-wait until `atomic_load_explicit(&a.scheduled, .Acquire) == 0 && atomic_load_explicit(&a.in_step, .Acquire) == 0` so no scheduler shard or worker holds a live reference to `a`.
3. Under `s.registry_mu`, swap-remove `a` from `s.owned_actors` and clear `a.reg_sched = nil`.
4. Drain remaining envelopes from `a.mailbox` (freeing `ENVELOPE_FLAG_HEAP` nodes, returning slab nodes to pools, and failing queued `ENVELOPE_FLAG_CALL` waiters with `.Target_Stopped`).
5. Destroy `a.monitors` and `a.reset_desc` (calling `vmem.arena_destroy(&desc.arena)` and freeing `init_state_copy`), then call `a.destroy_proc(a)`.

---

### 2.2 Thread-Safe Generic `Registry($Key)` with Duplicate Guard & Auto-Unenroll (`spec_27`)

**Where:** `actor/supervision.odin`, `actor/doc.odin` (`references/16_actor_destroy.odin`, `17_registry.odin`)

1. **Types & Procedures:**
   - `Registry :: struct($Key: typeid) { mu: sync.RW_Mutex, entries: map[Key]rawptr, allocator: mem.Allocator }`
   - `registry_init(r: ^Registry($Key), allocator := context.allocator)`
   - `registry_destroy(r: ^Registry($Key))`
   - `registry_register(r: ^Registry($Key), key: Key, target: PID($Msg) / ^Actor($State, $Msg)) -> bool`
   - `registry_unregister(r: ^Registry($Key), key: Key)`
   - `registry_lookup(r: ^Registry($Key), key: Key, $Msg: typeid) -> (PID(Msg), bool)`
2. **Live-Owner Duplicate Guard & Auto-Unenrollment on Permanent Stop (`spec_27`):**
   - `registry_register(reg, key, target)` checks whether `key` is already bound to a live `.Running` actor; if so, it returns `false` without overwriting the existing live owner (even if `target` differs).
   - Actors maintain lightweight registry backlinks (or clean unenrollment hooks triggered on permanent `.Stopped` transition inside `handle_actor_fault`, `actor_base_destroy`, and `stop_supervisor`) so permanently stopped actors (`.Normal` exit, `.Temporary` crash, `supervisor_terminate_child`, or supervisor meltdown) automatically remove their `Registry($Key)` bindings and drain incoming `Monitor_Node` edges without waiting for `scheduler_destroy`.
   - When a supervised actor restarts in place (`on_reinit`), its `Registry($Key)` entry remains bound to the live incremented generation `PID` or updates cleanly if re-registered in `on_reinit`.
3. **Global Convenience Registry:**
   - Expose global `default_registry: Registry(u64)` (initialized inside package runtime lifecycle or `actor_runtime_init`/`scheduler` & `actor.init` / `actor.destroy` in `actor/doc.odin`, or explicit overload group so `mem.Tracking_Allocator` reports `0` leaks on every test!).
   - Provide `register_name`, `unregister_name`, `whereis_name($Msg)` overloads.
4. **Rule of Thumb on Hot Paths:** Only tenant routing / name resolution acquires `sync.RW_Mutex` (`sync.rw_mutex_shared_lock`); direct `actor.send(pid, msg)` remains 100% lock-free.

---

### 2.3 Sharded Multi-Tenant `Partitioned_Supervisor` & `Dynamic_Supervisor` (`spec_17`, `spec_22`, `spec_25`)

**Where:** `actor/supervision.odin`, `tests/supervision_spec_test.odin`, `tests/supervision_dsl_test.odin`, `tests/supervision_driver_test.odin` (`references/18_dynamic_supervisor.odin`, `19_test_22_dynamic_supervisor.odin`, `20_supervision_world_dyn_sup.odin`, `.tmp/reference_repos/elixir/lib/elixir/lib/partition_supervisor.ex`)

1. **Sharded Architecture (`Partitioned_Supervisor`):**
   - Partition table of size `num_partitions` (default `DEFAULT_PARTITIONS :: 64`, configurable) indexed by `part_idx := org_id % u64(len(ps.partitions))`.
   - Each `Tenant_Partition` (`#align(64)`) owns its own `sync.RW_Mutex`, `tenants: map[u64]^Tenant_Entry`, and optional shard-level `Supervisor` / clock reference.
   - Support `max_children: int` (`<= 0` means unbounded) in `Partitioned_Supervisor_Config`, returning `.Max_Children_Reached` when active capacity is exhausted (`spec_25`).
   - Eliminates global single-mutex contention when 100+ organizations concurrently spawn, route messages, and reap idle tenants.

2. **Safe Concurrent Send/Call vs Eviction & `.Temporary` Reclamation (`in_flight`, Two-Phase Sweep, `spec_22`):**
   - `Tenant_Entry` tracks:
     - `org_id: u64`
     - `sup_pid: PID(Supervisor_Msg)` + `sup_base: ^Actor_Base`
     - `worker_bases: [dynamic]^Actor_Base`
     - `last_active: time.Tick`
     - `in_flight: i32` (atomic counter)
     - `evicting: bool`
   - **Routing API (`get_or_spawn_tenant`, `send_to_tenant`, `try_call_tenant`, `tenant_count`):**
     - `get_or_spawn_tenant(ps, org_id, factory_proc, user_ctx)` looks up or spawns a fresh isolated tenant supervision tree under `part.mu` (enforcing `max_children`), updates `last_active`, and registers the tenant route atomically.
     - `send_to_tenant` and `try_call_tenant` acquire shared lock on `part.mu`, locate non-evicting `entry`, increment `entry.in_flight` (`atomic_add`), release `part.mu` *before* blocking inside `try_call`, perform the send/call, and decrement `entry.in_flight` on return—preventing any use-after-free if a concurrent sweeper attempts eviction.
   - **`.Temporary` Worker & Spec Reclamation (`spec_22`):**
     - `.Temporary` workers spawned under `Partitioned_Supervisor` / `Dynamic_Supervisor` that exit cleanly or crash immediately release their `Tenant_Entry` / `Child_Spec` / `Reset_Desc` memory via `actor_base_destroy`, leaving `0` dead retained specs (`500` crashed `.Temporary` children retain `0` dead allocations and leak `0 B`).
   - **Two-Phase `sweep_idle_tenants(ps, idle_ttl, now)`:**
     - **Phase 1 (Under `part.mu` Lock):** Scan entries; skip any entry with `atomic_load(&entry.in_flight) > 0` or `time.tick_diff(entry.last_active, now) < idle_ttl`. For eligible entries, verify every actor (`sup_base` + `worker_bases`) has `scheduled == 0 && in_step == 0` (or mark `evicting = true`), append eligible keys/entries to a local slice `evict_list`, and erase keys from `part.tenants` **after** the map iteration finishes (never mutating an Odin `map` with `delete_key` inside `for k, v in part.tenants`).
     - **Phase 2 (Unlocked / Reclaim):** Destroy each evicted tenant's worker actors and supervisor actor via `actor_base_destroy` (waiting cleanly if any final turn was draining), destroy `worker_bases`, free `Tenant_Entry`, and return evicted count.

3. **`Dynamic_Supervisor` Compatibility API:**
   - Provide `Dynamic_Supervisor :: Partitioned_Supervisor` alias and `dynamic_supervisor_init` / `dynamic_supervisor_destroy` / `dynamic_supervisor_get_or_spawn_tenant` / `dynamic_supervisor_sweep_idle_tenants` / `dynamic_supervisor_count` wrappers so callers written for either `Partitioned_Supervisor` or `Dynamic_Supervisor` work identically with full partition sharding.

4. **ATDD `spec_17` (`tests/supervision_spec_test.odin`):**
   - Turn Track 2's `spec_17_partitioned_supervisor_isolation_and_idle_eviction` Green: concurrent tenant isolation, `Virtual_Clock` idle eviction, `in_flight` call protection against concurrent sweep, and `0` memory leaks under `mem.Tracking_Allocator`.

---

### 2.4 `@(supervise)` & `@(reinit)` AST Codegen + Negative Diagnostics (`actor/codegen/codegen.odin`, `test_04`–`test_06`)

**Where:** `actor/doc.odin`, `actor/codegen/codegen.odin`, `tools/sleipnir_gen`, `tests/codegen_test.odin`, `examples/05_ast_metaprogramming/` (`references/07_supervision_allocator_alias.odin`–`13_codegen_supervise_reinit_test.odin`)

1. **`actor.Allocator :: mem.Allocator` Alias (`actor/doc.odin`):**
   - Export `Allocator :: mem.Allocator` in `package actor` so generated `*_gen.odin` files can reference `actor.Allocator` without requiring `"core:mem"` imports when user files do not import `core:mem`.

2. **AST Attribute Parsing (`actor/codegen/codegen.odin`):**
   - `Reinit_Spec :: struct { actor_name: string, proc_name: string, takes_alloc: bool }` stored on `Actor_Spec.reinit` and resolved via `pending_reinits: ^[dynamic]Reinit_Spec` in `finalize_module_spec`.
   - `Supervise_Spec :: struct { enabled: bool, strategy: string, restart: string, max_restarts: int, window_ms: int, use_arena: bool }` parsed from `@(supervise)` / `@(supervise="strategy=one_for_one,restart=permanent,use_arena=true")` struct attributes.
   - `parse_source_into_spec`:
     - Recognize `@(supervise)` alongside `@(actor)`.
     - Recognize `@(reinit)` procedures (`proc(self: ^Actor_Type)` or `proc(self: ^Actor_Type, alloc: mem.Allocator)`), validate parameter signatures cleanly, and **exclude** `@(reinit)` procedures from being turned into `<Actor>_Msg` message union variants!

3. **Structured Negative AST Validation (`validate_module_spec` — `test_05`, `test_06`):**
   - Implement `validate_module_spec(spec: ^Module_Spec, allocator := context.allocator) -> ([]Diagnostic, bool)` reporting structured `file`, `line`, and `message` diagnostics before emitting Odin files, and wire diagnostic printing + non-zero exit codes into `tools/sleipnir_gen` (`main.odin`):
     - **`test_05`:** Reject `@(supervise="strategy=unknown_mode")`, `@(supervise="max_restarts=-1")`, multiple `@(reinit)` procedures targeting the same `@(actor)` struct, `@(reinit)` with mismatched receiver pointer type, and procedures annotated with both `@(behavior)` and `@(reinit)`.
     - **`test_06`:** Reject `@(behavior)` procedures declaring ambiguous/duplicate `reply_to` parameters or malformed `reply_to: actor.Reply($T)` syntax cleanly without generating broken Odin code.

4. **Code Emission (`emit_module_code` — `test_04`):**
   - Whenever an actor has `@(reinit)` or `@(supervise)`:
     - Generate `private` / internal `_<snake_actor>_reinit_trampoline :: proc(raw_self: rawptr, alloc: actor.Allocator)` calling the user `@(reinit)` proc cleanly (passing 1 or 2 args according to `takes_alloc`).
     - Generate `<Actor>_DEFAULT_RESTART_POLICY :: actor.Restart_Policy.<...>` from `@(supervise)` config.
     - Generate overloaded `spawn_<snake_actor>_sup`:
       - `spawn_<snake_actor>_sup(sup: ^actor.Supervisor, init_state: <Actor> = <Actor>{}, policy := <Actor>_DEFAULT_RESTART_POLICY, use_arena := <...>, id: u32 = 0) -> <Actor>_Actor` (registering the actor under `sup` with the generated reinit trampoline and configured arena isolation).
   - Turn Track 2's `test_04`, `test_05`, and `test_06` Green in `tests/codegen_test.odin`.

---

### 2.5 Full Linearizability (`prop_29`), Multi-Tenant Demo (`examples/06_otp_supervision.odin`) & CI (`.github/workflows/ci.yml`)

1. **Multi-Threaded Fault-Injection & Linearizability (`prop_29`):**
   - Turn Track 2's `prop_29_multithreaded_fault_injection_supervision_linearizability` (`4` OS worker threads x `50` deterministic PRNG seeds across `send`, `try_call`, `actor.forward`, `monitor`, `registry_register`/`lookup`, child admin, `panic`, `fail_turn`, `.Normal` stop) Green across all 7 invariants (`active_actors == 0`, no orphaned running descendants, monotonic generations, 100% monitor delivery, zero dangling registry entries, zero blocked `try_call` waiters, `0 B` leaked) in debug and `-o:speed` builds.

2. **`examples/06_otp_supervision.odin` (`references/21_06_otp_supervision.odin` + `OTP carbon review.md` fixes):**
   - Demonstrate 50 concurrent organizations x 3 pipeline workers (`Validator -> Enricher -> Writer`, 150 active workers + 50 tenant supervisors under `Partitioned_Supervisor`).
   - Use `.Rest_For_One` strategy so a panic in `Validator` stops `Writer` & `Enricher` in reverse order and reinitializes the pipeline cleanly forward (`Enricher` + `Writer` stay synchronized).
   - Use `actor.current_generation()` for generation tracking (`no unsafe pointer cast` from `ctx.current_env` or `State` internals), `Partitioned_Supervisor` / `Dynamic_Supervisor` thread-safe `try_call_tenant` / `send_to_tenant` (zero unsafe external reads of actor memory while workers run), `try_call` error handling on injected failures, and `sweep_idle_tenants` cleanup exiting with `0` leaks.

3. **`ols.json` & `.github/workflows/ci.yml` (`references/22_ci_workflow.yml`):**
   - Ensure `ols.json` has `"checker_args": "-custom-attribute:actor,behavior,no_behavior,supervise,reinit"`.
   - Update `.github/workflows/ci.yml` to run:
     - `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit` (and `-o:speed`)
     - `odin check ./actor -vet -strict-style -disallow-do -warnings-as-errors`
     - `odin check ./tests -vet -strict-style -disallow-do -warnings-as-errors`
     - `odin run tools/sleipnir_gen -- examples/05_ast_metaprogramming` + `git diff --exit-code` + `odin test examples/05_ast_metaprogramming -custom-attribute:actor,behavior,no_behavior,supervise,reinit`
     - `odin run examples/06_otp_supervision.odin -file -o:speed`

---

## 3. Acceptance Criteria

1. Track 2's full specification suite (`spec_01`–`spec_28`, `prop_18`, `prop_29`, `test_01`–`test_06`) passes 100% (Green) under `odin test ./tests -custom-attribute:actor,behavior,no_behavior,supervise,reinit` in both debug and `-o:speed` builds with `0` failures and `0` memory leaks under `mem.Tracking_Allocator`.
2. `examples/06_otp_supervision.odin` runs cleanly to completion in both debug and `-o:speed` modes with `0` leaked bytes (`mem.Tracking_Allocator`) and clean output.
3. Full CI workflow commands pass locally under `-vet -strict-style -disallow-do -warnings-as-errors`.
4. No files inside `conductor/tracks/otp_supervision_20261115/references/` are modified.
