# Dignus.ActorServer.Rust

[English](Usage.md) | 한국어

[한국어 개요](../README.ko.md)

C# Dignus.ActorServer를 Rust 소유권·스레드·수명·모듈 규칙에 맞춘 포트다. 기존 런타임 구조를 가능한 한 유지하며 불필요한 추상화나 재설계를 피한다.

## 워크스페이스와 시작

dignus-actor-core는 디스패처·메일박스·Ask, dignus-actor-network는 mio 다중 Reactor TCP/TLS를 제공한다. 네트워크 패키지 이름은 dignus-actor-server다. crates.io 최초 릴리스 전에는 Git 의존성을 사용한다.

```toml
[dependencies]
dignus-actor-core = "0.1"
dignus-actor-server = "0.1"
```

```toml
[dependencies]
dignus-actor-core = { git = "https://github.com/EomTaeWook/Dignus.ActorServer.Rust" }
dignus-actor-server = {
    git = "https://github.com/EomTaeWook/Dignus.ActorServer.Rust",
    package = "dignus-actor-server"
}
```

```bash
cargo run -p dignus-actor-core --example basic
cargo run -p dignus-actor-server --example tcp_echo
```

```rust
use dignus_actor_core::actor_ref_trait::ActorRefTrait;

actor_ref.post(Box::new(MyMessage), None);
```

```rust
use std::time::Duration;

system.shutdown_timeout(Duration::from_secs(5))?;
```

```rust
let shutdown = host.shutdown_handle();
let server = std::thread::spawn(move || host.run());

// Later:
shutdown.shutdown();
server.join().unwrap()?;
```

메시지는 ActorMessageTrait를 구현하는 소유 값이다. ActorBase를 구현해 ActorSystem으로 생성하고 ActorRefTrait의 post·kill을 사용한다. 종료에는 shutdown_timeout으로 제한 시간을 지정한다. 네트워크 호스트는 차단 실행이므로 서버 스레드로 넘기기 전에 shutdown_handle을 얻고 종료 후 join한다.

## C#과 Rust 대응

namespace는 mod, internal은 pub(crate), interface·추상 기반은 trait, IDisposable은 명시적 dispose로 대응한다. ThreadStatic은 thread_local!, 동기화는 Arc·Mutex·atomic, 스레드는 JoinHandle과 전용 디스패처로 구성한다. 기존 SynchronizationContext·Thread.Priority·백그라운드 설정·참조 기반 풀을 표준 라이브러리에 그대로 옮길 수 없으므로 필요한 범위에서 조정한다.

## 실행과 대기

Post → 메일박스 → 소유 디스패처 예약·신호 → 실행 큐 배출 → receive 순서다. receive가 Pending이면 다음 메시지 처리를 멈추고 저장된 상태를 유지한다. wake가 Actor 실행을 다시 예약하며 Pending을 먼저 poll한다. Ready가 되면 정상 처리를 재개한다.

ActorAwait::join(target).await는 continuation의 디스패처 문맥만 바꾸며 Actor 소유권을 넘기지 않는다. Actor 상태에 접근하기 전 올바른 문맥으로 돌아오거나 확인해야 한다.

## 종료와 안전 조건

Kill은 종료 상태로 전환하고 새 메시지를 거부하지만 Pending receive를 강제 취소하지 않는다. receive 완료와 소유 디스패처 복귀 후 종료가 확정된다. 영원히 완료하지 않는 작업은 Actor 종료도 막는다.

동시에 receive 하나만 활성화하고 Pending 중 메일박스 진행·중복 poll·종료 확정을 하지 않는다. 상태에 접근하는 실행 문맥은 구현자가 지킨다.

## 런타임 구성

ActorSystem은 디스패처·생성·라우팅·종료를 관리한다. Actor context는 Self 참조와 문맥 확인, 참조는 post·kill, runner는 인스턴스·메일박스·대기 receive·종료 확정을 소유한다. dispatcher는 스케줄 큐·전용 스레드, await는 continuation 전환을 담당한다.

## 네트워크

mio Poll 루프별 I/O 작업자에 연결을 고정한다. TcpHost·TlsHost, 저수준 HostHandler, 연결마다 Actor를 생성하는 SessionActorHost를 제공한다. Decoder와 length-prefix 프레이밍, FrameEncoder와 SessionSender를 사용한다. TLS는 rustls ring backend이며 같은 Reactor 엔진에서 Transport만 달라진다. HostOptions로 작업자 수·메일박스·송신 버퍼 한도를 구성한다. 다른 스레드의 송신은 Waker와 대기 쓰기 큐로 소유 Reactor에 전달한다.

## 벤치마크

[한국어 측정 방법·환경·전체 수치](../benchmarks/README.ko.md)를 참조한다. 아래는 원문의 요약 표다. 순수 프로세스 내 측정과 네트워크 echo는 다른 부하다.

| Framework | Throughput (msg/s) |
| --- | --- |
| **`dignus-actor-core` (this project)** | **~365–400M** |
| actix 0.13 | ~195–206M |
| ractor 0.14 | ~107M |
| kameo 0.20 | ~70–90M |
| xtra 0.6 | ~83–85M |
| coerce 0.8 | ~58–80M |


| Server | config | msg/s |
| --- | --- | ---: |
| C# (raw, no actor) | thread pool | ~63M |
| tokio (raw, no actor) | 8 workers | ~62M |
| Dignus Rust (raw, no actor) | 8 io-workers | ~61M |
| **Dignus Rust (actor)** | 2 disp + 6 io | **~60–64M** |
| actix (actor) | 8 arbiters | ~55M |
| **Dignus C# (actor)** | 2 dispatchers | **~54M** |


## 기존 C# 기준값

i5-12400F 6코어·12스레드, RAM 32GB, Windows x64의 기존 측정이다. Rust와 같은 장비가 아니므로 직접 비교하지 않는다. 동일 32스레드 장비 재측정 C#은 약 620–640M, Rust는 약 365–400M이며 아래 기존 277M과 구분한다. 네트워크·직렬화·DB·로깅·게임 로직을 포함하지 않고 메시지별 전역 동기화도 하지 않았다. CPU·스케줄링·전원·GC·워밍업에 따라 달라진다.

```text
CPU: Intel Core i5-12400F
RAM: 32 GB
OS: Windows x64
```

```text
Actor Pair Count: 348
Actual Actor Count: 696
Pipeline Size Per Pair: 1,000
Benchmark Duration: 10 seconds
Counter: per-actor local counter, summed after completion
```

```text
Processed Messages: 2,907,908,768
Elapsed: 10.475 sec
Throughput: 277,599,493 msg/s
```

```text
Throughput: around 250M ~ 270M msg/s
```

## 빌드와 구성

워크스페이스 루트 또는 dignus-actor-core에서 확인 명령을 실행할 수 있다. 아래는 사용자를 위한 명령 설명이다.

```bash
cargo check
```

```bash
cargo check
```

```toml
[workspace]
members = [
    "dignus-actor-core",
    "dignus-actor-network"
]
```

```toml
[package]
name = "dignus-actor-core"
version = "0.1.0"
edition = "2021"

[dependencies]
```

## 라이선스

MIT 라이선스. [LICENSE](../LICENSE)를 참조한다.
