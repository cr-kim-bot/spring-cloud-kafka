# Kafka Topics CLI

Kafka는 토픽 관리를 위한 **`kafka-topics.sh`** CLI 도구를 제공한다.

## 컨테이너 접속

```bash
docker exec -it kafka bash
```

- 접속 후 `/opt/kafka/bin` 디렉토리에 위치해야 함
- `./kafka-topics.sh` 로 사용 가능한 옵션 확인 가능

## 주요 옵션

| 옵션 | 설명 |
|---|---|
| `--bootstrap-server` | 연결할 Kafka 서버 (필수) |
| `--create` | 토픽 생성 |
| `--delete` | 토픽 삭제 |
| `--list` | 전체 토픽 목록 조회 |
| `--describe` | 토픽 상세 정보 조회 |
| `--topic` | 대상 토픽 이름 지정 |

> `--bootstrap-server`는 모든 명령에 필수. localhost라도 명시해야 하며 Spring Boot처럼 자동 추론하지 않는다.

## 토픽 생성

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-events
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic payment-events
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic shipping-events
```

## 토픽 삭제

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --delete
```

## 전체 토픽 목록 조회

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## 토픽 상세 정보 조회

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --describe
```

- 출력 항목: Partition Count, Replication Factor, Leader, Replicas 등
- 각 항목의 의미는 이후 강의에서 설명

# Offset

Kafka 토픽은 **append-only 불변 구조**다. 메시지는 수신된 순서대로 저장되며 수정·삭제되지 않는다.

각 메시지에는 배열 인덱스처럼 **고유한 순번(offset)**이 부여된다.

```
offset:  0       1       2       3  ...
       [msg0] [msg1] [msg2] [msg3]
```

- 첫 메시지는 offset 0부터 시작, 이후 메시지마다 1씩 증가
- 컨슈머는 offset 순서대로 메시지를 수신 → **수신 순서 보장**
- offset 최대값은 `Long.MAX_VALUE` (9,223,372,036,854,775,807) — 초당 100만 건을 발행해도 소진하려면 약 292,000년 소요

> Partition 개념과 함께 사용되며, 파티션별로 offset이 독립적으로 관리된다. (이후 강의에서 설명)