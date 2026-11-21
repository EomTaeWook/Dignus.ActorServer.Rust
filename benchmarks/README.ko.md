# 벤치마크

[English](README.md) | 한국어

[한국어 개요](../README.ko.md) | [런타임 가이드](../docs/Usage.ko.md)

프로세스 내 ping-pong, Ask 요청·응답, TCP echo를 각각 측정한다. 앞의 두 측정은 순수 디스패치 마이크로벤치마크이며 네트워크·직렬화·DB·업무 로직이 지배하는 실제 서비스 성능을 대표하지 않는다. TCP는 클라이언트와 서버가 같은 장비여서 변동이 크다.

## 프로세스 내 ping-pong

동일 장비·방법·날짜의 3회 범위다. 비교한 Rust 프레임워크 안에서 해당 측정의 순위이며 가능한 모든 구현의 최고 성능을 뜻하지 않는다. C#의 620–640M은 같은 32스레드 장비 재측정값이다. 원래 i5-12400F의 277M과 직접 비교하지 않는다.

| Framework | Version | Throughput (msg/s) |
| --- | --- | --- |
| **`dignus-actor-core` (this project)** | — | **~365–400M** |
| actix | 0.13.5 | ~195–206M |
| ractor | 0.14.7 | ~107M |
| kameo | 0.20.0 | ~70–90M |
| xtra | 0.6.0 | ~83–85M |
| coerce | 0.8.11 | ~58–80M |


| Reference | Throughput (msg/s) | Notes |
| --- | --- | --- |
| C# `Dignus.ActorServer` (re-run here) | ~620–640M | the C# benchmark re-run on **this same 32-thread machine**. (The original README's ~277M is on a weaker i5-12400F / 6c-12t — not comparable.) |


## Ask 요청·응답

대상 Actor 32개마다 동시 Ask 루프 1024개로 총 32768개를 진행하며 10초 동안 완료 응답을 센다. 호출자별 카운터를 사용하고 공유 완료 atomic을 두지 않았다. 결과는 3회 범위다.

| Framework | Version | Throughput (ask/s) |
| --- | --- | --- |
| **`dignus-actor-core` (this project)** | — | **~50–60M** |
| ractor | 0.14.7 | ~30–36M |
| actix | 0.13.5 | ~30–36M |
| coerce | 0.8.11 | ~28–40M |
| xtra | 0.6.0 | ~30M |
| kameo | 0.20.0 | ~24–28M |


| Reference | Version | Throughput (ask/s) | Notes |
| --- | --- | --- | --- |
| C# `Dignus.ActorServer` | — | ~24M | actor-asker; `TaskCompletionSource` reply, shared timeout sweeper |
| Akka.NET | 1.5.69 | ~0.7M | `Ask` allocates a temp reply actor (`PromiseActorRef`) per call |
| Proto.Actor | 1.8.0 | ~0.3M | `RequestAsync` allocates a future process per call |


tokio 기반 프레임워크는 경량 task에서 oneshot으로 응답을 받는다. Dignus는 호출자 Actor에서 Ask를 실행하고 디스패처로 재개한다. 회신 참조가 awaiter를 직접 보유해 응답의 레지스트리 조회·잠금을 피한다. 타임아웃은 기본 2^18 슬롯 링과 전용 스위퍼로 관리하며 with_capacities로 설정한다. 요청 id·슬롯 배정의 공유 카운터와 응답 잠금 제거가 해당 측정의 차이 요인이다. Akka·Proto 결과에는 호출별 회신 Actor·future 프로세스 할당 비용이 포함된다.

## TCP echo

32바이트 고정 메시지, 연결당 window 1000의 closed-loop echo다. 응답을 받으면 다시 보내 파이프라인을 유지한다. 동일 echo_client에 서버만 바꾼다. 일반 방법은 10초 구간이지만 고정 affinity 비교 표는 20초 실행의 중앙값이다.

서버는 코어 0–7, 클라이언트는 8–31(24연결)에 배정한다. 런타임별로 8코어 예산 내 최적 스레드 구성을 탐색했다. i9-14900K 32논리코어, Windows TCP loopback, .NET 10·Cargo Release의 동일 장비 조건이다.

**Targets:**

| Target | IO model | actor/codec |
| --- | --- | --- |
| Dignus Rust (actor) | mio multi-reactor (per-core Poll, connection-pinned) | yes (SessionActorHost + codec + EchoActor) |
| Dignus Rust (raw) | 〃 | no (echo inside the reactor) |
| **actix (actor)** | tokio, one actor per connection pinned to an arbiter thread | yes (actix `StreamHandler` + `FramedWrite`) |
| Dignus C# (actor) | SAEA (IOCP) + .NET thread pool + `Task.Run` per send | yes |
| tokio (raw) | tokio multi-thread runtime (task-per-conn) | no |


**Peak-vs-peak (each at its 8-core optimum, same session, median):**

| Server | config | msg/s |
| --- | --- | ---: |
| C# (raw, no actor) | thread pool | ~63M |
| tokio (raw, no actor) | 8 workers | ~62M |
| Dignus Rust (raw, no actor) | 8 io-workers | ~61M |
| **Dignus Rust (actor)** | 2 disp + 6 io | **~60–64M** |
| actix (actor) | 8 arbiters | ~55M |
| **Dignus C# (actor)** | 2 dispatchers | **~54M** |


| Rust `disp + io` | msg/s |   | C# `dispatchers` | msg/s |
| --- | ---: | --- | --- | ---: |
| 2 + 6 (peak) | ~66M |   | 2 (peak) | ~53M |
| 4 + 4 | ~47M |   | 4 | ~50M |
| 6 + 2 | ~33M |   | 8 | ~34M |
| 2 + 2 | ~32M |   | 16 | ~28M |


raw는 Actor·코덱 없는 echo, actor는 세션 Actor 경로다. actix는 연결별 Actor와 arbiter, tokio raw는 task-per-connection을 사용한다. 다른 비교 Rust Actor 프레임워크는 전용 TCP 계층이 없어 tokio와 actix를 네트워크 비교 대상으로 삼았다.

서버·클라이언트가 같은 장비라 세션별 비율도 약 ±15%, 단일 실행은 ±30% 변동할 수 있다. 중앙값을 사용하며 안정적 비교에는 별도 장비가 필요하다. 큰 메시지·무거운 핸들러에서는 echo가 강조하는 I/O 경로 차이가 줄어든다.

### 재현 명령과 affinity

benchmarks 디렉터리에서 필요한 서버 하나와 클라이언트를 선택한다. 아래 명령의 인자값과 표의 최적값은 구분한다. 서버 affinity 마스크는 255, 클라이언트는 4294967040이다. 최적 설정은 Rust 2 dispatcher+6 I/O, tokio 8, actix 8, C# dispatcher 2다.



## 프로세스 내 측정 방법

348쌍, 총 696개 Actor가 상대 주소를 보유한다. 쌍마다 1000개 메시지를 미리 넣고 수신 시 Actor별 카운터를 올린 뒤 fire-and-forget으로 하나를 회신한다. 10초 후 중단해 모든 카운터를 합산하고 total_processed/elapsed_seconds로 계산한다. 작업 스레드는 논리 CPU 수이며 Release의 opt-level=3, lto=true, codegen-units=1이다.

환경은 32논리코어, Windows x64의 WSL2, Cargo 1.96.0이다. 원문 날짜는 ping-pong 2026-06-19, Ask 2026-06-25다.

## 재현

각 비교 crate는 독립 워크스페이스다. benchmarks에서 아래 명령을 실행한다. Ask는 각 crate의 --bin ask를 사용한다. Processed 또는 Completed, Elapsed, Throughput을 여러 번 기록해 범위로 제시한다.



## 수치 해석

- 순수 디스패치 결과를 실제 서비스 처리량으로 사용하지 않는다.
- 요청·응답, supervision, 분산과 사용성 등 프레임워크의 다른 목표·비용을 평가한 결과가 아니다.
- ping-pong과 Ask의 경로·순위를 분리한다.
- 스케줄링·터보·백그라운드 부하에 따라 달라진다.
- 비교한 Rust 프레임워크는 5개뿐이다.
- 같은 장비의 C# 결과는 더 높다. Rust의 dyn 디스패치는 열린 메시지 API를 제공하는 대신 일부 비용을 감수한다.
