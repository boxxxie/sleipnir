# Sleipnir Testing & 4-Layer ATDD Architecture Guide

Sleipnir uses **Acceptance Test-Driven Development (ATDD)** and **Dave Farley's 4-Layer Executable Specification Architecture** (popularized during the development of the LMAX Disruptor) across both its low-level lock-free concurrency tests ([`actor/actor_test.odin`](../actor/actor_test.odin)), AST metaprogramming generator tests ([`actor/codegen/codegen_test.odin`](../actor/codegen/codegen_test.odin)), and end-to-end DSL specifications ([`examples/05_ast_metaprogramming/dsl_test.odin`](../examples/05_ast_metaprogramming/dsl_test.odin)).

---

## Running the Test Suites

Every test in Sleipnir runs inside a `with_leak_check` harness backed by `core:mem.Tracking_Allocator`, verifying **zero leaked bytes** and **zero invalid frees** after every test case:

```bash
# 1. Core runtime, MPSC/SPSC mailboxes, pools, & M:N scheduler (19 tests):
odin test ./actor

# 2. AST metaprogramming parser & code generator (3 tests):
odin test ./actor/codegen

# 3. End-to-end generated `@(actor)` DSL runtime specification:
odin test examples/05_ast_metaprogramming -custom-attribute:actor,behavior
```

---

## The 4-Layer ATDD Architecture in Odin

Traditional concurrency tests mix **what** is being specified (deadlock freedom, message routing, batch atomicity, state transitions) with **how** it is observed (spawning throwaway sink actors, counting cumulative global messages, copying state out of scope blocks before scheduler teardown, and wiring up `Tracking_Allocator`).

Sleipnir separates tests into four distinct layers:

```
┌───────────────────────────────────────────────────────────────────────────┐
│ Layer 1: Executable Specifications (`@(test)` procedures)                 │
│   Declarative `given_*` / `upon_*` / `then_*` steps.                      │
│   Contains ALL concrete test inputs & expected outputs—zero plumbing.     │
├───────────────────────────────────────────────────────────────────────────┤
│ Layer 2: Domain / Scenario DSL (`given_*`, `upon_*`, `then_*`)            │
│   Expresses scenario intent (e.g., `upon_depositing_via_selector`,        │
│   `upon_orchestrating_square`, `then_balance_is`).                        │
├───────────────────────────────────────────────────────────────────────────┤
│ Layer 3: Protocol Driver & Test Probes (`Runtime_Spec`, `given_probe`)    │
│   Translates DSL calls into System Under Test API calls, manages          │
│   `scoped_scheduler`, tracks per-step message deltas (`rt.total_msgs`),   │
│   spawns reusable `Probe($T)` actors, and runs cross-cutting assertions.  │
├───────────────────────────────────────────────────────────────────────────┤
│ Layer 4: System Under Test (`package actor` & `package actor_codegen`)    │
│   Lock-free MPSC/SPSC queues, `Envelope_Pool`, `Scheduler`, `Actor`,      │
│   `PID`, and `sleipnir_gen` AST codegen.                                  │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Key Design Principles

### 1. Zero Hidden Test Data in Layers 2 & 3
All concrete test inputs (`9`, `10`, `7`, `11`, `{100, 200}`, `balance = 500`) and expected outputs (`91`, `49`, `121`, `675`, `750`) live **explicitly in Layer 1**. Layer 2 and Layer 3 helpers parameterize inputs rather than hardcoding magic numbers inside fixtures.

### 2. Per-Step Message Deltas Instead of Fragile Cumulative Counts
In low-level runtime tests ([`actor/actor_test.odin`](../actor/actor_test.odin)), `wait_messages(n)` waits until the scheduler's lifetime message counter reaches `n`. Rather than forcing Layer 1 tests to manually track cumulative totals (`after_messages = 3`, `5`, `7`), Layer 3's `Runtime_Spec` accumulates `total_msgs` internally so Layer 1 only states the **delta for that specific step**:

```odin
@(private = "file")
Runtime_Spec :: struct {
	t:          ^testing.T,
	sched:      ^Scheduler,
	total_msgs: i64,
}

@(private = "file")
then_actor_state_is :: proc(
	rt:       ^Runtime_Spec,
	target:   ^Actor($S, $M),
	expected: S,
	messages: i64 = 0, // Per-step delta, not cumulative lifetime total!
) {
	if messages > 0 {
		rt.total_msgs += messages
		wait_messages(rt.total_msgs)
	}
	wait_idle()
	testing.expect_value(rt.t, target.state, expected)
}
```

### 3. Reusable Test Probes (`given_probe`) Instead of Ad-Hoc Sink Actors
When testing asynchronous reply forwarding (`send(target, msg, reply_to)`), Layer 1 does not manually spawn throwaway `sink1`, `sink2`, `sink3` actors. Instead, Layer 3 provides `given_probe(&rt, $T)` and `then_async_reply_resolves`:

```odin
then_async_reply_resolves(&rt, math_worker,
	req      = Compute_Square{x = 7},
	expected = 49,
	via      = .Actor_Handle,
)
then_async_reply_resolves(&rt, math_worker,
	req      = Compute_Square{x = 11},
	expected = 121,
	via      = .PID,
)
```

### 4. Cross-Cutting Overload Verification in Layer 3 Drivers
In [`examples/05_ast_metaprogramming/dsl_test.odin`](../examples/05_ast_metaprogramming/dsl_test.odin), a single Layer 1 assertion (`then_balance_is(&s, 750)` or `then_audit_log_matches(&s, entries = 1, last_balance = 750)`) automatically verifies **every generated dispatch path** in Layer 3:
1. Built-in Odin selector syntax (`s.account->get_balance()`)
2. Synchronous `call_<behavior>` with the `<Actor>_Actor` struct (`call_get_balance(s.account)`)
3. Synchronous `call_<behavior>` with a state-erased `actor.PID` (`call_get_balance(actor.pid(s.account.raw))`)
4. Asynchronous `send_<behavior>` reply forwarding via `_reply_actor` (`send_get_balance(s.account, actor.pid(reply_actor_sink))`)
5. Asynchronous `send_<behavior>` reply forwarding via `_reply_pid` (`send_get_balance(actor.pid(s.account.raw), actor.pid(reply_pid_sink))`)

### 5. Explicit vs. Ambient Scheduler Coverage in Helper Signatures
Because Sleipnir provides both explicit-scheduler overloads (`spawn(sched, ...)`) and ambient thread-local overloads (`spawn(...)`, `send(...)`), Layer 2/3 helper signatures intentionally reflect which API surface they exercise:
- Helpers testing explicit scheduler passing accept `rt: ^Runtime_Spec` and pass `rt.sched`.
- Helpers testing ambient thread-local lookup omit `rt: ^Runtime_Spec` and call `spawn(...)` / `send(...)` directly.

### 6. Separating Scenario-Specific Fixtures (Layer 2) from Suite-Wide Drivers (Layer 3)
In [`actor/actor_test.odin`](../actor/actor_test.odin):
- **Layer 2 (Scenario-Specific Fixtures):** One-off actor behaviors and message types tailored to a specific concurrency edge case (such as `Compute_Square`, `Orchestrator_Run`, and `given_square_orchestrator` for single-worker cooperative `call` stepping in `test_18`) live immediately below their tests.
- **Layer 3 (Suite-Wide Protocol Driver):** Generic concurrency primitives (`Runtime_Spec`, `Addressing_Mode`, `given_probe`, `given_accumulator`, `upon_sending`, `upon_sending_batch_to_pid`, `upon_destroying_early`, `then_async_reply_resolves`, `then_actor_state_is`) live in the shared driver section for reuse across the entire test suite.
