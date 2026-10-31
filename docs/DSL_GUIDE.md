# Sleipnir Actor DSL & AST Metaprogramming Guide

Sleipnir (`package actor`) offers **two ergonomic tiers** for writing actors in Odin:

1. **Pure-Library Runtime DSL (`package actor`):** Zero code generation required. Uses Odin's `@(deferred_out)` RAII attributes, `proc` overload groups, ambient thread-local context, `base:intrinsics` structural typing (`spawn_self`), and envelope-header reply routing (`actor.call` / `actor.reply`).
2. **AST Metaprogramming DSL (`@(actor)` + `tools/sleipnir_gen`):** An **optional** pre-build step powered by Odin's standard library (`core:odin/parser` + `core:odin/ast`). You write plain structs marked `@(actor)` and ordinary Odin procedures, and `sleipnir_gen` generates the message structs, tagged `union`, `switch` dispatcher, `spawn_<actor>` constructor, `<Actor>_VTable` (for `actor->method(...)` syntax), and `send_<behavior>` / `call_<behavior>` overload groups into `actors_gen.odin`.

---

## Quick Comparison: Hand-Written vs. AST Metaprogramming

| Feature | Pure-Library Runtime DSL (`actor`) | AST Metaprogramming DSL (`@(actor)`) |
| :--- | :--- | :--- |
| **Build Step** | None (`odin run .`) | `odin run tools/sleipnir_gen -- <pkg_dir>` |
| **Compiler Flag** | None | `-custom-attribute:actor,behavior,no_behavior` |
| **Defining State** | `Bank_Account :: struct { ... }` | `@(actor) Bank_Account :: struct { ... }` |
| **Defining Behaviors** | Message structs + `union` + `switch` handler | Plain procs: `deposit :: proc(self: ^Bank_Account, ...)` |
| **1-Way Async Send** | `actor.send(account, Deposit{amount = 250})` | `account->deposit(250)` or `send_deposit(account, 250)` |
| **Request / Reply** | `actor.reply(val)` + `actor.call(account, Get_Balance{}, i64)` | `return self.balance` + `account->get_balance()` or `call_get_balance(account)` |
| **Default Arguments** | Manual struct defaults | Preserved automatically (`memo: string = "deposit"`) |

---

## Part 1: AST Metaprogramming Syntax (`@(actor)`)

### 1. Declaring an Actor (`@(actor)`)

Mark any struct in your package with `@(actor)`:

```odin
package main

import "actor"

@(actor)
Bank_Account :: struct {
    owner:    string,
    balance:  i64,
    tx_count: int,
}
```

Wait—where is `@(behavior)`? **It is optional!** Once `Bank_Account` is marked `@(actor)`, `sleipnir_gen` automatically discovers every procedure in the same package whose `self` parameter is `^Bank_Account`.

---

### 2. Defining 1-Way Asynchronous Behaviors (`send_`)

Any procedure taking `self: ^Bank_Account` (or `(ctx: ^actor.Context, self: ^Bank_Account, ...)`) with **no return value** becomes a 1-way asynchronous fire-and-forget behavior:

```odin
// 1-way behavior with a default parameter `memo = "deposit"`
deposit :: proc(self: ^Bank_Account, amount: i64, memo: string = "deposit") {
    self.balance  += amount
    self.tx_count += 1
}

// 1-way behavior
withdraw :: proc(self: ^Bank_Account, amount: i64) {
    self.balance  -= amount
    self.tx_count += 1
}
```

If a behavior needs explicit access to the worker's `^actor.Context`, put `ctx: ^actor.Context` as the first parameter and `self: ^Bank_Account` as the second:

```odin
deposit_with_ctx :: proc(ctx: ^actor.Context, self: ^Bank_Account, amount: i64) {
    self.balance += amount
}
```

> **Note:** Inside any behavior handler, Sleipnir already binds the active `^actor.Context` and `^actor.Scheduler` in thread-local storage, so you can call `actor.send(...)`, `actor.reply(...)`, `actor.forward(...)`, or `actor.complete_token()` directly without needing `ctx: ^actor.Context` in the signature.

---

### 3. Defining Request/Reply Behaviors (`-> Return_Type` / `call_`)

In traditional actor libraries, returning a value from an actor requires adding a `reply_to: actor.PID(T)` field to the message struct, spawning a temporary callback actor, and waiting on tokens.

In Sleipnir, **just give your procedure a return type (`-> T`) and `return` normally**:

```odin
get_balance :: proc(self: ^Bank_Account) -> i64 {
    return self.balance
}
```

Under the hood, `sleipnir_gen` generates:
1. A clean zero-field message struct (`Bank_Account_Msg_get_balance :: struct {}`).
2. Dispatcher code that calls `res := get_balance(self)` and passes `res` to `actor.reply(ctx, res)`, which routes the return value through the **message `Envelope` header** (`reply_slot`).
3. Both **synchronous** (`account->get_balance()` / `call_get_balance(target) -> i64`) and **asynchronous** (`send_get_balance(target, reply_to_pid)`) callers accepting either `Bank_Account_Actor` or `actor.PID(Bank_Account_Msg)`!

---

### 4. Calling Actor Behaviors (`->` vs `send_` / `call_`)

Once you spawn an actor with the generated `spawn_<actor_snake_name>` constructor, you have two ways to invoke its behaviors:

#### Style A: Odin's Built-in `->` Method Call Operator

Every generated `<Actor>_Actor` handle embeds `using vtable: ^<Actor>_VTable`, which enables Odin's selector call syntax `actor->behavior(...)`—complete with **default parameter values**:

```odin
account := spawn_bank_account(Bank_Account{owner = "Sleipnir", balance = 500})

// 1-way async sends (`deposit` and `withdraw` have no return value):
account->deposit(250)             // Uses default `memo = "deposit"`
account->deposit(100, "wire-in")  // Explicit `memo`
account->withdraw(175)

// Synchronous request/reply (`get_balance` returns `-> i64`):
balance := account->get_balance() // Blocks caller until actor processes prior messages -> 675!
```

#### Style B: `send_<behavior>` and `call_<behavior>` Procedure Overload Groups

`sleipnir_gen` also emits package-level overloaded procedure groups prefixed with `send_` (for asynchronous message sends) and `call_` (for synchronous request-reply calls):

```odin
// 1-way async sends (accepts either `Bank_Account_Actor` or raw `actor.PID(Bank_Account_Msg)`):
send_deposit(account, 250)
send_deposit(account, 100, "wire-in")
send_withdraw(account, 175)

// Synchronous request/reply call (accepts `Bank_Account_Actor` or `actor.PID(Bank_Account_Msg)`):
balance := call_get_balance(account)
balance  = call_get_balance(actor.pid(account.raw))

// Asynchronous request/reply forwarding (accepts `Bank_Account_Actor` or `actor.PID(Bank_Account_Msg)`):
send_get_balance(account, actor.pid(printer_actor))
send_get_balance(actor.pid(account.raw), actor.pid(printer_actor))
```

---

### 5. Multi-File Packages & Shared Behavior Names (`@(behavior="...")`)

In Odin, a directory is a single package where top-level procedure names must be unique across files.

- **One actor per file (`bank_account.odin`, `audit_logger.odin`, `main.odin`):** `sleipnir_gen` parses all `.odin` files in the directory in a two-pass scan, automatically matching procedures to `@(actor)` structs across files and forwarding any cross-file `import` declarations used in parameter or return types into `actors_gen.odin`.
- **Sharing a behavior name across multiple actors (`@(behavior="reset")`):** If two actors in the same package both need a `reset` behavior, give the Odin procedures distinct names (`account_reset` and `logger_reset`) and tag them with `@(behavior="reset")`:

```odin
// In bank_account.odin:
@(behavior="reset")
account_reset :: proc(self: ^Bank_Account) {
    self.balance  = 0
    self.tx_count = 0
}

// In audit_logger.odin:
@(behavior="reset")
logger_reset :: proc(self: ^Audit_Logger) {
    self.entries_logged = 0
}
```

`sleipnir_gen` merges both into a single overloaded `send_reset` procedure group **and** gives both actors a `.reset` method slot:

```odin
account->reset() // OR: send_reset(account)
logger->reset()  // OR: send_reset(logger)
```

- **Helper procedures (`@(no_behavior)` or `@(private)`):** If you write an internal helper procedure taking `self: ^Bank_Account` that should **not** be exposed as an asynchronous actor message, mark it `@(no_behavior)` or `@(private)`.

---

### 6. Running the Code Generator (`tools/sleipnir_gen`)

```bash
# 1. Generate `<dir>/actors_gen.odin` for one or more package directories:
odin run tools/sleipnir_gen -- examples/05_ast_metaprogramming

# 2. Build or run your package with Odin's `-custom-attribute` flag:
odin run examples/05_ast_metaprogramming -custom-attribute:actor,behavior,no_behavior -o:speed
```

---

## Part 2: Pure-Library Runtime DSL (`package actor`)

Even when you don't use `tools/sleipnir_gen`, Sleipnir's core library provides a clean, low-boilerplate DSL:

### 1. Scoped RAII Scheduler (`actor.scoped_scheduler`)

Uses Odin's `@(deferred_out = scheduler_scope_end)` attribute to start worker threads and automatically wait for idle, stop threads, and free all spawned actors and auto-managed `Envelope_Pool` slabs when the block exits:

```odin
main :: proc() {
    _ = actor.scoped_scheduler(num_workers = 4)

    // Spawn actors, send messages, call actors...
} // <-- Automatically waits for idle and frees all actors/pools/threads here!
```

### 2.Flexible `actor.spawn` Signatures & `actor.spawn_self`

`actor.spawn` is an overload group supporting 4 handler styles:

```odin
// 1. 2-arg pointer handler `(self: ^State, msg: ^Msg)` — best for zero-copy `actor.forward`
a1 := actor.spawn(My_State{}, proc(self: ^My_State, msg: ^My_Msg) { ... })

// 2. 2-arg value handler `(self: ^State, msg: Msg)` — cleanest for `switch m in msg` on a `union`
a2 := actor.spawn(My_State{}, proc(self: ^My_State, msg: My_Msg) { ... })

// 3. 3-arg explicit context handler `(ctx: ^actor.Context, self: ^State, msg: ^Msg)`
a3 := actor.spawn(My_State{}, proc(ctx: ^actor.Context, self: ^My_State, msg: ^My_Msg) { ... })

// 4. Structural typing via `actor.spawn_self` (`base:intrinsics.type_has_field(T, "receive")`):
Counter :: struct {
    total:   i64,
    receive: proc(self: ^Counter, delta: i64),
}
a4 := actor.spawn_self(Counter{
    receive = proc(self: ^Counter, delta: i64) { self.total += delta },
})
```

### 3. Envelope-Header Request/Reply (`actor.call` & `actor.reply`)

Why shouldn't message structs carry `reply_to: actor.PID(Res)`?
1. It couples the **payload** (what you are asking) with the **transport routing** (who is asking and whether they want a synchronous return or an async message).
2. In Sleipnir, the [`Envelope($Msg)`](../actor/pool.odin) header carries an internal `reply_slot` pointer and routing flags (`ENVELOPE_FLAG_CALL` or `ENVELOPE_FLAG_REPLY_PID`).

Inside your handler, simply call [`actor.reply(value)`](../actor/actor.odin):

```odin
Deposit     :: struct { amount: i64 }
Get_Balance :: struct {} // Zero `reply_to` field!

Account_Msg :: union { Deposit, Get_Balance }

account := actor.spawn(i64(100), proc(balance: ^i64, msg: Account_Msg) {
    switch m in msg {
    case Deposit:
        balance^ += m.amount
    case Get_Balance:
        actor.reply(balance^) // Routes to `actor.call` caller OR `reply_to` PID!
    }
})

// Option A: Synchronous request/reply (`actor.call`)
current := actor.call(account, Account_Msg(Get_Balance{}), i64)

// Option B: Asynchronous reply forwarding (`actor.send` with 3rd `reply_to` argument)
actor.send(account, Account_Msg(Get_Balance{}), printer_pid)
```

---

## Testing Your Actors (4-Layer ATDD Architecture)

For patterns on testing both low-level `package actor` concurrency topologies and `@(actor)` AST metaprogramming behaviors with zero memory leaks, see the [**Sleipnir Testing & 4-Layer ATDD Architecture Guide (`tests/README.md`)**](../tests/README.md) (`odin test ./tests`).
