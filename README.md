# Dignus.ActorServer.Rust

English | [한국어](README.ko.md)

Rust actor runtime and TCP/TLS server implementation of the Dignus actor model.

## Requirements

Rust 1.80 or later, edition 2021, and Cargo. Workspace members are declared in Cargo.toml.

## Installation

```toml
[dependencies]
dignus-actor-core = { git = "https://github.com/EomTaeWook/Dignus.ActorServer.Rust" }
dignus-actor-server = { git = "https://github.com/EomTaeWook/Dignus.ActorServer.Rust", package = "dignus-actor-server" }
```

## Quick start

Run the existing actor and TCP examples from the workspace root.

```bash
cargo run -p dignus-actor-core --example basic
cargo run -p dignus-actor-server --example tcp_echo
```

## Usage

The detailed guide covers ownership, mailbox ordering, pending receives, dispatcher switching, shutdown deadlines, and network hosts. Benchmark results retain their original environment and interpretation limits.

## Documentation

- [Runtime and network guide](docs/Usage.md)
- [Actor example](dignus-actor-core/examples/basic.rs)
- [Benchmark methodology](benchmarks/README.md)

## License

MIT License. See [LICENSE](LICENSE).
