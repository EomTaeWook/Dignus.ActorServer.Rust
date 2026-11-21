# Dignus.ActorServer.Rust

[English](README.md) | 한국어

Dignus Actor 모델의 Rust 런타임과 TCP/TLS 서버 구현.

## 요구 사항

Rust 1.80 이상, edition 2021과 Cargo가 필요하다. 워크스페이스 구성은 Cargo.toml을 따른다.

## 설치

```toml
[dependencies]
dignus-actor-core = { git = "https://github.com/EomTaeWook/Dignus.ActorServer.Rust" }
dignus-actor-server = { git = "https://github.com/EomTaeWook/Dignus.ActorServer.Rust", package = "dignus-actor-server" }
```

## 빠른 시작

워크스페이스 루트에서 기존 Actor·TCP 예제를 실행한다.

```bash
cargo run -p dignus-actor-core --example basic
cargo run -p dignus-actor-server --example tcp_echo
```

## 사용법

소유권, 메일박스 순서, 대기 중 수신, 디스패처 전환, 종료 제한 시간과 네트워크 호스트는 상세 가이드를 따른다. 벤치마크 결과는 기존 환경과 해석 범위를 함께 보존한다.

## 문서

- [런타임·네트워크 가이드](docs/Usage.ko.md)
- [Actor 실행 예제](dignus-actor-core/examples/basic.rs)
- [벤치마크 방법](benchmarks/README.ko.md)

## 라이선스

MIT 라이선스. [LICENSE](LICENSE)를 참조한다.
