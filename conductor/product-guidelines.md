# Product Guidelines

## Design Principles
- **Zero Memory Leaks:** Every test suite and example must pass `mem.Tracking_Allocator` with `0` leaked allocations and `0` bad frees.
- **Hot-Path Performance:** Keep `actor.send` and `scheduler_worker_proc` lock-free; avoid global registry locks on the hot message-send path (prefer in-place restart via `Actor_Base` + `vmem.Arena` reset).
- **Idiomatic Odin:** Use `context.allocator`, `context.temp_allocator`, `@(deferred_out)` RAII guards, and compile-time polymorphism (`$State`, `$Msg`) without hidden runtime allocations.
- **4-Layer ATDD Testing:** Follow the architecture in `tests/README.md`:
  1. Executable Specifications (`@(test)` procs)
  2. Domain-Specific Language (DSL helpers with `loc := #caller_location`)
  3. Protocol Drivers (`World` structs wrapping `actor.*` APIs)
  4. System Under Test (`package actor`)
