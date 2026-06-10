# Dignus.ActorServer.Rust

Rust port of the Dignus Actor Framework.

This repository is a work-in-progress port of the original C# `Dignus.ActorServer` project to Rust.
The goal is to keep the original actor runtime design as close as possible while adapting the implementation to Rust ownership, threading, and module rules.

---

## Porting Target

Original project:

```text
Dignus.ActorServer
├─ Dignus.Actor.Abstractions
├─ Dignus.Actor.Core
├─ Dignus.Actor.Network
└─ Benchmark
```

Rust target:

```text
Dignus.ActorServer.Rust
├─ actor-abstractions
├─ actor-core
├─ actor-network
└─ benchmark
```

---

## Current Status

This project is currently in the early porting stage.

Current focus:

- `Dignus.Actor.Core`
- Actor dispatcher
- Actor scheduling
- Yield continuation task
- Object pool structure
- Rust module layout

Not yet completed:

- Full actor lifecycle
- Actor references
- Ask/request-response flow
- Network layer
- Protocol layer
- Benchmarks

---

## Design Direction

The Rust implementation follows these rules:

- Keep the original C# structure where practical
- Avoid unnecessary abstraction during porting
- Prefer direct Rust equivalents over redesign
- Use Rust modules instead of C# namespaces
- Use `Arc`, `Mutex`, atomics, and thread-local storage where required
- Keep dispatcher execution dedicated to worker threads

---

## Actor Core Layout

Current `actor-core` structure:

```text
actor-core
├─ Cargo.toml
└─ src
   ├─ lib.rs
   ├─ actor_system.rs
   ├─ dispatcher
   │  ├─ mod.rs
   │  ├─ actor_dispatcher.rs
   │  ├─ actor_yield_task.rs
   │  └─ signal.rs
   ├─ internals
   │  ├─ mod.rs
   │  └─ actor_schedulable.rs
   └─ object_pool
      ├─ mod.rs
      └─ actor_yield_task_pool.rs
```

---

## C# to Rust Mapping

| C# | Rust |
|---|---|
| `namespace` | `mod` |
| `internal` | `pub(crate)` |
| `interface` | `trait` |
| `IDisposable` | explicit `dispose()` or `Drop` |
| `[ThreadStatic]` | `thread_local!` |
| `SemaphoreSlim` | `Mutex` + `Condvar` based signal |
| `Thread` | `std::thread::JoinHandle` |
| `volatile bool` | `AtomicBool` |
| `Interlocked` | atomic operations |
| `ObjectPoolBase<T>` | `Mutex<Vec<Arc<T>>>` based pool |
| `SendOrPostCallback + state` | `FnOnce()` closure |

---

## Actor Dispatcher

The dispatcher keeps the original execution idea:

```text
Schedule actor
   ↓
Signal dispatcher thread
   ↓
Worker thread wakes up
   ↓
Drain scheduled actor queue
   ↓
Execute actor schedulable items
```

Current dispatcher components:

- `ActorDispatcher`
- `ActorSchedulable`
- `ActorYieldTask`
- `ActorYieldTaskPool`
- `Signal`

---

## Build

From the workspace root:

```bash
cargo check
```

Or from `actor-core`:

```bash
cargo check
```

---

## Workspace Example

Root `Cargo.toml`:

```toml
[workspace]
members = [
    "actor-core"
]
```

`actor-core/Cargo.toml`:

```toml
[package]
name = "actor-core"
version = "0.1.0"
edition = "2021"

[dependencies]
```

---

## Notes

Some C# features do not have direct Rust standard library equivalents.

Examples:

- `SynchronizationContext`
- `Thread.Priority`
- Background thread setting
- C# object reference based pooling

These are ported only when the surrounding Rust structure requires them.

---

## License

Licensed under the MIT License.
See `LICENSE` in the project root.
