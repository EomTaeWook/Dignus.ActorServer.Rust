# Benchmarks

Reproducible throughput benchmarks for the Dignus Rust port, in three families:

1. **In-process ping-pong** (fire-and-forget) — `dignus-actor-core` vs five mainstream
   Rust actor frameworks.
2. **Ask** (request/response) — same set plus C# / Akka.NET / Proto.Actor.
3. **Network echo (TCP)** — `dignus-actor-server` (mio multi-reactor) vs tokio and the
   C# `Dignus.ActorServer`.

> ⚠️ **Read the caveats before quoting any number.** (1) and (2) are *pure in-process
> dispatch microbenchmarks* and do **not** represent real-world performance, which is
> dominated by network/serialization/DB/logic, not actor dispatch. Each framework
> optimizes for different things (ask, supervision, distribution, ergonomics).
> (3) is co-located (client+server on one box), so throughput is noisy — see its caveats.

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

## Ask throughput (request/response)

A separate benchmark for the **ask** (request → typed reply) path, same machine,
same day. 32 target actors, **1,024 concurrent ask loops each** (32,768 in-flight),
10s, each loop doing the framework's request/reply call and counting completed
replies. **Per-asker counters** (no shared completion atomic, so the measurement
itself isn't a bottleneck). Ranges over 3 runs.

| Framework | Version | Throughput (ask/s) |
| --- | --- | --- |
| **`dignus-actor-core` (this project)** | — | **~50–60M** |
| ractor | 0.14.7 | ~30–36M |
| actix | 0.13.5 | ~30–36M |
| coerce | 0.8.11 | ~28–40M |
| xtra | 0.6.0 | ~30M |
| kameo | 0.20.0 | ~24–28M |

For context (not Rust actor frameworks):

| Reference | Version | Throughput (ask/s) | Notes |
| --- | --- | --- | --- |
| C# `Dignus.ActorServer` | — | ~24M | actor-asker; `TaskCompletionSource` reply, shared timeout sweeper |
| Akka.NET | 1.5.69 | ~0.7M | `Ask` allocates a temp reply actor (`PromiseActorRef`) per call |
| Proto.Actor | 1.8.0 | ~0.3M | `RequestAsync` allocates a future process per call |

**Idiom & design.** The five tokio frameworks issue each ask from a lightweight
task (`call` / `ask` / `send().await`) with a oneshot reply. `dignus` has no
external runtime, so each ask loop is an **asker actor** (`ask().await` resuming
on its dispatcher), and the reply is **lock-free**: the reply actor-ref holds the
awaiter directly and calls `set_response` — no registry lookup or lock on the hot
path. Timeouts are tracked in a fixed slot ring (default 2¹⁸, configurable via
`ActorSystem::with_capacities`) swept by one background thread.

**Takeaway.** On the same actor-ask pattern, `dignus-actor-core` leads at
~50–60M. The decisive factor was removing **every per-ask shared atomic/lock**
(the request-id counter, the slot-assignment counter, and the reply-side lock) —
the same "no shared synchronization on the hot path" principle that makes its
fire-and-forget path fast. The mainstream C# frameworks (Akka/Proto) are 50–150×
slower here because their ask allocates a per-call reply actor / future process.

## Network echo throughput (TCP)

Throughput of the network layer (`dignus-actor-server`, mio multi-reactor) against
other actor-network IO models, on an echo workload. This measures the **IO
architecture**; echo maximizes per-message overhead and thread hand-offs, so it
surfaces the network-layer difference most sharply.

**Method** (mirrors the Dignus C# `TcpTestClient`):

- 32-byte fixed messages, closed-loop echo; **window 1000** per connection seeded up
  front, each message re-sent as its echo returns (pipeline kept full).
- **10 s** window; bytes echoed back → `msg/s`.
- **One load client (`echo_client`), server swapped** = apples-to-apples.

**Targets:**

| Target | IO model | actor/codec |
| --- | --- | --- |
| Dignus Rust (actor) | mio multi-reactor (per-core Poll, connection-pinned) | yes (SessionActorHost + codec + EchoActor) |
| Dignus Rust (raw) | 〃 | no (echo inside the reactor) |
| **actix (actor)** | tokio, one actor per connection pinned to an arbiter thread | yes (actix `StreamHandler` + `FramedWrite`) |
| Dignus C# (actor) | SAEA (IOCP) + .NET thread pool + `Task.Run` per send | yes |
| tokio (raw) | tokio multi-thread runtime (task-per-conn) | no |

> Most Rust actor frameworks (ractor/kameo/xtra/coerce) ship **no TCP layer** — they run
> on tokio. **actix** is the exception (built-in actor + IO integration), so it's the
> concrete "tokio-based actor network" here; **tokio (raw)** is the IO ceiling underneath
> them all.

**Setup:** pinned — server → cores 0–7, load client → cores 8–31 (24 connections); 20 s
runs, median. (Unpinned/shared numbers are useless — same config swings 25–94M run-to-run.)
Each runtime is swept to its own 8-core optimum, since they thread differently (Rust sets
io-workers + dispatchers explicitly; C# has dispatchers + a hidden .NET thread pool → few
dispatchers).

**Peak-vs-peak (each at its 8-core optimum, same session, median):**

| Server | config | msg/s |
| --- | --- | ---: |
| C# (raw, no actor) | thread pool | ~63M |
| tokio (raw, no actor) | 8 workers | ~62M |
| Dignus Rust (raw, no actor) | 8 io-workers | ~61M |
| **Dignus Rust (actor)** | 2 disp + 6 io | **~60–64M** |
| actix (actor) | 8 arbiters | ~55M |
| **Dignus C# (actor)** | 2 dispatchers | **~54M** |

Raw layers ~tied (~61–63M); actor path ~10–15% less. **Rust actor ~1.1–1.25× over C#
actor** — close; exact ratio noise-limited on a co-located box (±15% session-to-session).

**8-core config sweep** (each runtime is sensitive to thread count vs core budget):

| Rust `disp + io` | msg/s |   | C# `dispatchers` | msg/s |
| --- | ---: | --- | --- | ---: |
| 2 + 6 (peak) | ~66M |   | 2 (peak) | ~53M |
| 4 + 4 | ~47M |   | 4 | ~50M |
| 6 + 2 | ~33M |   | 8 | ~34M |
| 2 + 2 | ~32M |   | 16 | ~28M |

**Environment:** i9-14900K (32 logical cores), TCP loopback, Windows (net10.0 / cargo
release), client + server on the **same machine**.

**Reproduce** (from `benchmarks/`):

```bash
# server (pick one)
cd dignus-network-echo && cargo run --release --bin echo_server     -- 5000 32 8  # full (port, dispatchers, io-workers)
cd dignus-network-echo && cargo run --release --bin echo_server_raw -- 5000 8      # raw  (port, io-workers)
cd tokio-echo          && cargo run --release                       -- 5000 32     # tokio raw (port, worker_threads)
cd actix-echo          && cargo run --release                       -- 5000 8      # actix actor (port, arbiters)
# Dignus C#: Dignus.ActorServer-main/Benchmark/TcpActorServer → dotnet build -c Release, then
#            tail -f /dev/null | ./bin/Release/net10.0/TcpActorServer.exe   # Console.Read EOF guard

# load client (addr, connections, window, duration_secs)
cd dignus-network-echo && cargo run --release --bin echo_client -- 127.0.0.1:5000 16 1000 10

# context-switch sampling during load:
powershell -File dignus-network-echo/sample.ps1 <process-name>
```

Pinning: set affinity via `(Get-Process -Id <pid>).ProcessorAffinity = [IntPtr]<mask>`
(server `255` = cores 0–7, client `4294967040` = cores 8–31). Per-server 8-core optimum:
Rust `echo_server 5000 2 6`, tokio `8`, actix `8`, C# `.WithDispatcherThreads(2)`.

**Caveats:** co-located client+server → single-run swings ±30% (use medians; a clean number
needs separate machines); each runtime must be sized to the core budget (see the sweep);
echo amplifies the IO layer, so large messages / heavy handlers shrink all gaps.

## Methodology (in-process ping-pong / ask)

Every in-process benchmark does the identical thing:

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
- Date: 2026-06-19 (ping-pong); 2026-06-25 (ask)

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

The **ask** benchmark is a second binary in every crate (`--bin ask`):

```bash
cd dignus.actor-core-pingpong && cargo run --release --bin ask
cd actix-pingpong   && cargo run --release --bin ask
cd ractor-pingpong  && cargo run --release --bin ask
cd kameo-pingpong   && cargo run --release --bin ask
cd xtra-pingpong    && cargo run --release --bin ask
cd coerce-pingpong  && cargo run --release --bin ask
```

Each prints `Processed`/`Completed`, `Elapsed`, and `Throughput`. Run a few times;
report a range.

## Caveats (please keep these attached to the numbers)

1. **Microbenchmark only.** Pure in-process dispatch. Real services are I/O- and
   logic-bound; this number is invisible there.
2. **Different goals.** actix/kameo/coerce/xtra/ractor provide request-reply,
   supervision trees, distribution, and richer ergonomics. Those features are
   not free, and this benchmark does not exercise or credit them.
3. **Two paths, two rankings.** Fire-and-forget (ping-pong) and request/reply
   (ask) are separate benchmarks — don't quote one as "the" number.
4. **Variance.** Scheduling/turbo/background load swing results; hence ranges.
5. **Not exhaustive.** Only five frameworks were tested; others exist.
6. **Higher ceilings exist.** The C# original reaches ~620–640M on this machine;
   `dignus-actor-core`'s `dyn`-dispatch path trades some of that for an open,
   ergonomic message API.
