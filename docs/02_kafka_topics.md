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

## Offset 확인 데모

### 클린 상태로 Kafka 재시작

이전 토픽/오프셋 상태를 제거하고 새로 시작한다. 볼륨 매핑이 없으면 데이터가 초기화된다.

```bash
docker compose down
docker compose up
```

### 토픽 생성 및 컨슈머 시작

```bash
# 토픽 생성
./kafka-topics.sh --bootstrap-server localhost:9092 --topic demo-topic --create

# 컨슈머 실행 (offset 출력 활성화)
./kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic demo-topic \
    --property print.offset=true
```

- `print.offset=true`: 수신 메시지와 함께 offset 번호를 출력

### 프로듀서 시작 (별도 터미널)

```bash
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-topic
```

메시지를 입력하면 컨슈머 측에서 `offset: 0, hi` / `offset: 1, hello` 형태로 출력된다.

### 타임스탬프 함께 출력

`print.timestamp=true` 속성을 추가하면 메시지 생성 시각도 함께 출력된다.

```bash
./kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic demo-topic \
    --property print.offset=true \
    --property print.timestamp=true \
    --from-beginning
```

- 타임스탬프는 Unix time(ms)으로 출력되며 사람이 읽기 쉬운 형식으로 변환 가능