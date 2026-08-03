# Kafka Producer

## producer.properties

`/opt/kafka/config/producer.properties` — Kafka 서버 설정이 아닌 **클라이언트(프로듀서) 애플리케이션용** 설정 파일이다.

```
broker.properties      → Kafka 서버 (브로커 노드)
controller.properties  → Kafka 서버 (컨트롤러 노드)
server.properties      → Kafka 서버 (브로커+컨트롤러 노드)
producer.properties    → 프로듀서 클라이언트 애플리케이션
consumer.properties    → 컨슈머 클라이언트 애플리케이션
```

## Kafka Driver (Client Library)

프로듀서 애플리케이션은 직접 Kafka에 메시지를 쓰는 것이 아니라 내부적으로 **Kafka Client Library (Driver)**를 사용한다.

```
Console Producer  ──→  Kafka Driver  ──→  Kafka Topic
Java/Spring Boot  ──→  Kafka Driver  ──→  Kafka Topic
```

- Java/Spring Boot 애플리케이션 개발 시 동일한 드라이버를 의존성으로 포함
- 드라이버가 실제 전송 로직을 담당
- `producer.properties`는 이 드라이버의 동작을 제어하는 설정

## 배치 전송: linger.ms & batch.size

드라이버는 메시지를 즉시 전송하지 않고 모아서 **배치(batch)로 전송**하여 네트워크 효율을 높인다.

| 속성 | 기본값 | 설명 |
|---|---|---|
| `linger.ms` | `0` | 드라이버가 메시지를 수집하며 대기하는 최대 시간 (ms) |
| `batch.size` | `16384` (16KB) | 배치에 담을 수 있는 최대 바이트 수 |

**두 조건 중 먼저 충족되는 쪽에서 전송**

- `linger.ms` 시간이 지나면 → 모은 바이트가 적어도 즉시 전송
- `batch.size`가 채워지면 → 시간이 남아도 즉시 전송

# Kafka Console Producer

학습, 테스트, 디버깅 용도의 CLI 메시지 발행 도구. 프로덕션 용도로는 사용하지 않는다.

## 메시지 발행

```bash
./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo-topic
```

- 실행 후 `>` 프롬프트가 나타나면 메시지 입력 대기 상태
- 한 줄 입력 후 **Enter** → 해당 줄이 메시지 하나로 전송
- 메시지는 plain text로 전송
- `Ctrl + C` 로 종료

## 즉시 전송 설정

Console Producer의 기본 `linger.ms`는 1000ms(1초)로 오버라이드되어 배치 전송된다. `--timeout 0`으로 즉시 전송할 수 있다.

```bash
./kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic demo-topic \
  --timeout 0
```