# Benchmarks

In-process **ping-pong** message-throughput benchmarks for `dignus-actor-core`, plus the
same benchmark implemented on five mainstream Rust actor frameworks, so the
numbers are reproducible rather than asserted.

> ⚠️ **Read the caveats before quoting any number.** This is a *pure in-process
> dispatch microbenchmark*. It does **not** represent real-world performance,
> where work is dominated by network, serialization, database, and game logic —
> not actor dispatch. Each framework here optimizes for different things
> (ask/request-reply, supervision, distribution, ergonomics); this measures only
> raw local fire-and-forget message throughput.

## Results

Same machine, same methodology, same day. Numbers are ranges over 3 runs
(throughput is noisy on a busy many-core box).

| Framework | Version | Throughput (msg/s) |
| --- | --- | --- |
| **`dignus-actor-core` (this project)** | — | **~365–400M** |
| actix | 0.13.5 | ~195–206M |
| ractor | 0.14.7 | ~107M |
| kameo | 0.20.0 | ~70–90M |
| xtra | 0.6.0 | ~83–85M |
| coerce | 0.8.11 | ~58–80M |

For context (not Rust actor frameworks, shown to bound the scale):

| Reference | Throughput (msg/s) | Notes |
| --- | --- | --- |
| C# `Dignus.ActorServer` (re-run here) | ~620–640M | the C# benchmark re-run on **this same 32-thread machine**. (The original README's ~277M is on a weaker i5-12400F / 6c-12t — not comparable.) |

**Takeaway:** among the five Rust actor frameworks tested here, `dignus-actor-core`
is the fastest on this benchmark (~1.9× the next, actix; ~4–5× the others), using
its own `std::thread`-based dispatcher. The C# original goes higher still — so
this is "fastest *among the tested Rust frameworks on this benchmark*", not
"fastest possible".

## Methodology

Every benchmark does the identical thing:

- **348 ping/pong pairs** = 696 actors. Each actor holds its peer's address.
- **1000 in-flight messages seeded per pair** (the pipeline depth).
- On receive: if running, bump a per-actor counter and send **one** message back
  to the peer using the framework's **fire-and-forget** send
  (`post` / `do_send` / `tell` / `notify` — never a request/reply round-trip).
- **10-second** measured window; then stop and sum every actor's counter.
- Throughput = `total_processed / elapsed_seconds`.
- **Multi-threaded** runtime with worker threads = logical CPU count.
- Release build: `opt-level = 3`, `lto = true`, `codegen-units = 1`.

### Environment

- CPU: 32 logical cores
- OS: Windows x64 (run under WSL2), `cargo` 1.96.0
- Date: 2026-06-19

## How to reproduce

Each crate is standalone (its own workspace). From this `benchmarks/` directory:

```bash
# this project
cd dignus.actor-core-pingpong && cargo run --release

# the five Rust actor frameworks
cd actix-pingpong   && cargo run --release
cd ractor-pingpong  && cargo run --release
cd kameo-pingpong   && cargo run --release
cd xtra-pingpong    && cargo run --release
cd coerce-pingpong  && cargo run --release
```

Each prints `Processed`, `Elapsed`, and `Throughput`. Run a few times; report a
range.

## Caveats (please keep these attached to the numbers)

1. **Microbenchmark only.** Pure in-process dispatch. Real services are I/O- and
   logic-bound; this number is invisible there.
2. **Different goals.** actix/kameo/coerce/xtra/ractor provide request-reply,
   supervision trees, distribution, and richer ergonomics. Those features are
   not free, and this benchmark does not exercise or credit them.
3. **Fire-and-forget only.** No request/reply (ask) path is measured.
4. **Variance.** Scheduling/turbo/background load swing results; hence ranges.
5. **Not exhaustive.** Only five frameworks were tested; others exist.
6. **Higher ceilings exist.** The C# original reaches ~620–640M on this machine;
   `dignus-actor-core`'s `dyn`-dispatch path trades some of that for an open,
   ergonomic message API.
