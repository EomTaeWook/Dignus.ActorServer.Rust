# Dignus.ActorServer.Rust

A high-performance Rust port of the Dignus Actor Framework.

This repository is a work-in-progress port of the original C# [`Dignus.ActorServer`](https://github.com/EomTaeWook/Dignus.ActorServer) project to Rust.

The original C# runtime is designed for high-throughput local actor message processing and has reached around **250M ~ 270M msg/s** in local in-process ping-pong benchmarks, with a best observed result of **277M msg/s**.

The goal of this Rust port is to keep the original actor runtime design as close as possible while adapting the implementation to Rust ownership, threading, lifetime, and module rules.

---

## Porting Target

Original C# project:

```text
Dignus.ActorServer
├─ Dignus.Actor.Abstractions
├─ Dignus.Actor.Core
├─ Dignus.Actor.Network
└─ Benchmark
```

Rust port target:

```text
Dignus.ActorServer.Rust
├─ dignus-actor-core
├─ actor-network
└─ benchmark
```

Current workspace status:

```text
Dignus.ActorServer.Rust
├─ dignus-actor-core        (actor runtime: dispatchers, mailbox, ask)
├─ dignus-actor-network     (mio multi-reactor: TCP/TLS, session=actor, length-prefix codec)
└─ benchmarks               (in-process ping-pong + network echo)
```

## Quick Start

The workspace publishes two packages:

```toml
[dependencies]
dignus-actor-core = "0.1"
dignus-actor-server = "0.1"
```

Until the first crates.io release, use the Git repository:

```toml
[dependencies]
dignus-actor-core = { git = "https://github.com/EomTaeWook/Dignus.ActorServer.Rust" }
dignus-actor-server = {
    git = "https://github.com/EomTaeWook/Dignus.ActorServer.Rust",
    package = "dignus-actor-server"
}
```

Runnable examples:

```bash
cargo run -p dignus-actor-core --example basic
cargo run -p dignus-actor-server --example tcp_echo
```

Actor messages are owned values implementing `ActorMessageTrait`. Implement
`ActorBase` for actor state, spawn it through `ActorSystem`, then import
`ActorRefTrait` to post or kill:

```rust
use dignus_actor_core::actor_ref_trait::ActorRefTrait;

actor_ref.post(Box::new(MyMessage), None);
```

Application shutdown should use a deadline:

```rust
use std::time::Duration;

system.shutdown_timeout(Duration::from_secs(5))?;
```

TCP and TLS hosts are blocking servers. Obtain a shutdown handle before moving
the host into its server thread:

```rust
let shutdown = host.shutdown_handle();
let server = std::thread::spawn(move || host.run());

// Later:
shutdown.shutdown();
server.join().unwrap()?;
```

---

## Design Direction

The Rust implementation follows these rules:

* Keep the original C# structure where practical
* Avoid unnecessary abstraction during porting
* Prefer direct Rust equivalents over redesign
* Use Rust modules instead of C# namespaces
* Use `Arc`, `Mutex`, atomics, and thread-local storage where required
* Keep dispatcher execution dedicated to worker threads
* Hide runtime-only internals where Rust module visibility allows it
* Preserve the original actor scheduling and dispatcher model

---

## C# to Rust Mapping

| C#                            | Rust                               |
| ----------------------------- | ---------------------------------- |
| `namespace`                   | `mod`                              |
| `internal`                    | `pub(crate)`                       |
| `interface`                   | `trait`                            |
| `abstract class`              | `trait` + actor context            |
| `IDisposable`                 | explicit `dispose()`               |
| `[ThreadStatic]`              | `thread_local!`                    |
| `SemaphoreSlim`               | custom signal                      |
| `Thread`                      | `std::thread::JoinHandle`          |
| `volatile bool`               | `AtomicBool`                       |
| `Interlocked`                 | atomic operations                  |
| `SynchronizationContext.Post` | dispatcher continuation scheduling |

---

## Actor Runtime Flow

The dispatcher keeps the original execution idea:

```text
Post message
   ↓
Enqueue actor mail
   ↓
Schedule actor runner on owner dispatcher
   ↓
Signal dispatcher thread
   ↓
Dispatcher drains scheduled queue
   ↓
Actor runner executes actor receive logic
   ↓
Actor handles message
```

If the receive logic becomes pending, mailbox processing for that actor is stopped until the pending receive completes.

---

## Dispatcher Context Switching

The C# version supports dispatcher switching through:

```csharp
await ActorAwait.Join(actor);
```

The Rust port keeps the same concept:

```rust
ActorAwait::join(actor).await;
```

This schedules the continuation onto the target actor dispatcher.

`ActorAwait::join(target).await` is a dispatcher context switch. It does not transfer actor ownership.

If actor state is accessed after a dispatcher switch, the actor implementation is responsible for returning to the correct dispatcher or validating the context.

---

## Actor Receive Await Model

The Rust port keeps the C# actor continuation model where practical.

Actor receive logic may suspend and resume while preserving actor execution order.

```text
receive message
   ↓
access actor state
   ↓
await
   ↓
resume receive logic
   ↓
access actor state again
```

While an actor receive operation is pending, the actor does not process the next mailbox message.

---

## Pending Receive Handling

Pending receive handling is part of the actor execution model.

```text
Receive operation becomes pending
   ↓
Store pending receive state
   ↓
Stop mailbox processing for this actor
   ↓
Wake schedules actor execution again
   ↓
Pending receive is checked first
   ↓
Ready resumes normal mailbox processing
```

This preserves the single-message execution model of the original C# actor runtime.

---

## Kill Semantics

`Kill()` changes the actor lifecycle state and rejects new messages.

If the actor currently has a pending receive operation, the runtime does not forcibly cancel it. Finalization occurs only after the pending receive completes and the actor returns to its owner dispatcher.

If a receive operation never completes, actor finalization also does not complete. This follows the original C# runtime semantics and is considered the actor implementation's responsibility.

---

## Runtime Safety Notes

The runtime relies on the following internal invariants:

* Only one receive operation may be active per actor
* Mailbox processing is stopped while receive logic is pending
* Pending receive must not be polled concurrently
* Actor finalization must not run while receive logic is still pending
* Actor implementations should validate dispatcher context before accessing actor-owned mutable state after dispatcher switching

---

## Current Core Components

### `ActorSystem`

* owns dispatchers
* spawns actors
* routes posts and kills
* disposes actors and dispatchers

### Actor base/context

* defines actor receive behavior
* stores runtime actor context
* provides self reference and dispatcher verification

### Actor reference

* posts messages
* posts actor mail
* kills actor

### Actor runner

* owns actor instance
* owns mailbox
* executes actor receive logic
* manages pending receive state
* finalizes actor kill

### Actor dispatcher

* owns scheduled execution queue
* owns dispatcher thread
* runs scheduled actor work

### Actor await

* switches async continuation to another actor dispatcher

---

## Network Layer (`dignus-actor-server`)

A port of the C# `Dignus.Actor.Network` + `Dignus.Sockets` layers, built on **mio**
(no async runtime — matches the `std::thread` identity of the core).

* **`TcpHost` / `TlsHost`** — multi-reactor server: one `Poll` loop per IO worker,
  connections sharded across workers and pinned to one. Mirror C# `ActorTcpHost` / `ActorTlsHost`.
* **`HostHandler`** — low-level `on_accepted` / `on_data` / `on_disconnected` (C# `IActorHostHandler`).
* **`SessionActorHost`** — session = actor: spawns one actor per connection, decodes
  inbound frames, posts to the actor, kills on disconnect (C# `SessionActorBase` + `TcpServerBase`).
* **Codec** — `MessageDecoder` / `LengthPrefixedDecoder` (inbound) and
  `FrameEncoder` / `LengthPrefixedEncoder` (outbound), handed to the actor as a `SessionSender`.
* **TLS** — rustls (ring backend), sync state machine driven over mio.
* **`HostOptions`** — worker count (default = cores), mailbox capacity, send-buffer cap.

TCP and TLS share one internal `Reactor<H>` engine; only the `Transport` (plain vs
rustls) differs. Cross-thread sends from actors reach the owning reactor via a
`mio::Waker` + pending-writes queue.

---

## Benchmarks

Reproducible in-process **ping-pong** throughput benchmarks live in
[`benchmarks/`](./benchmarks), including the same benchmark implemented on five
mainstream Rust actor frameworks for comparison.

Same machine, same methodology (348 pairs / 1,000 pipeline / 10s, fire-and-forget),
3-run ranges:

| Framework | Throughput (msg/s) |
| --- | --- |
| **`dignus-actor-core` (this project)** | **~365–400M** |
| actix 0.13 | ~195–206M |
| ractor 0.14 | ~107M |
| kameo 0.20 | ~70–90M |
| xtra 0.6 | ~83–85M |
| coerce 0.8 | ~58–80M |

Among the Rust actor frameworks tested, `dignus-actor-core` is the fastest on this
benchmark, using its own `std::thread` dispatcher. The original C# runtime
(~620–640M on the same machine) reaches higher.

⚠️ This is a **pure in-process dispatch microbenchmark** — it does not reflect
real-world (network/DB/logic-bound) performance, and the frameworks optimize for
different goals. See [`benchmarks/README.md`](./benchmarks/README.md) for full
methodology, versions, environment, how to reproduce, and caveats.

### Network echo (TCP)

Network layer (`dignus-actor-server`, mio multi-reactor) vs other actor-network IO models,
32-byte closed-loop echo (window 1000, one load client, server swapped). Machine: i9-14900K
(32 cores), TCP loopback, co-located. Pinned — server → cores 0–7, client → cores 8–31 (24
connections); each server at its own 8-core optimum; 20 s runs, median.

| Server | config | msg/s |
| --- | --- | ---: |
| C# (raw, no actor) | thread pool | ~63M |
| tokio (raw, no actor) | 8 workers | ~62M |
| Dignus Rust (raw, no actor) | 8 io-workers | ~61M |
| **Dignus Rust (actor)** | 2 disp + 6 io | **~60–64M** |
| actix (actor) | 8 arbiters | ~55M |
| **Dignus C# (actor)** | 2 dispatchers | **~54M** |

Rust actor ~1.1–1.25× over C# actor; raw layers ~tied. Close — exact ratio noise-limited on
a co-located box. Full sweeps, method, and reproduce in
[`benchmarks/README.md`](./benchmarks/README.md#network-echo-throughput-tcp).

## Original C# Benchmark Baseline

This benchmark result is from the original C# `Dignus.ActorServer` implementation.

It is included as a baseline target for the Rust port.

### Benchmark

Local in-process ping-pong benchmark.

Test environment:

```text
CPU: Intel Core i5-12400F
RAM: 32 GB
OS: Windows x64
```

Benchmark conditions:

```text
Actor Pair Count: 348
Actual Actor Count: 696
Pipeline Size Per Pair: 1,000
Benchmark Duration: 10 seconds
Counter: per-actor local counter, summed after completion
```

Best observed result:

```text
Processed Messages: 2,907,908,768
Elapsed: 10.475 sec
Throughput: 277,599,493 msg/s
```

Representative result:

```text
Throughput: around 250M ~ 270M msg/s
```

> ⚠️ **Different machine — do not compare this number directly with the
> [Benchmarks](#benchmarks) table above.** This ~277M figure is from the original
> author's **Intel Core i5-12400F (6 cores / 12 threads)**, a weaker CPU with far
> fewer cores. The throughput scales with core count (more cores → more pairs run
> in parallel). For an apples-to-apples comparison, the same C# benchmark was
> re-run on the **same 32-thread machine** as the Rust benchmarks and reaches
> **~620–640M msg/s** (see [`benchmarks/`](./benchmarks)). On that same machine,
> C# (~640M) is still ahead of this Rust port (~380M).

Notes:

* This benchmark measures local actor message throughput only.
* It does not include network, serialization, database access, logging, or game logic.
* Per-message global synchronization was intentionally avoided.
* Results may vary depending on CPU scheduling, background processes, power mode, GC timing, and runtime warm-up.
* This is not the Rust port benchmark result.

---

## Build

From the workspace root:

```bash
cargo check
```

Or from `dignus-actor-core`:

```bash
cargo check
```

---

## Workspace

Root `Cargo.toml`:

```toml
[workspace]
members = [
    "dignus-actor-core",
    "dignus-actor-network"
]
```

`dignus-actor-core/Cargo.toml`:

```toml
[package]
name = "dignus-actor-core"
version = "0.1.0"
edition = "2021"

[dependencies]
```

---

## Notes

Some C# features do not have direct Rust standard library equivalents.

Examples:

* `abstract class`
* `protected`
* `SynchronizationContext`
* `Thread.Priority`
* Background thread setting
* C# reference-based object pooling

These are adapted only where the current Rust runtime structure requires them.

---

## License

Licensed under the MIT License.

See [`LICENSE`](./LICENSE) in the project root.
