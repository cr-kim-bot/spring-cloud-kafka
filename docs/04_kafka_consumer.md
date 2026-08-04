# Kafka Consumer

## consumer.properties

`/opt/kafka/config/consumer.properties` — Kafka 서버 설정이 아닌 **클라이언트(컨슈머) 애플리케이션용** 설정 파일이다.

# Pull 방식

컨슈머는 Kafka 브로커로부터 메시지를 **직접 가져온다(Pull)**. 브로커가 컨슈머에게 메시지를 밀어넣는(Push) 방식이 아니다.

## Push 방식을 사용하지 않는 이유

브로커와 컨슈머는 지속적인 TCP 연결을 유지하므로 기술적으로 Push가 가능하다. 그러나 Push 방식에는 문제가 있다.

- Kafka는 모든 메시지가 컨슈머에게 **안전하게 처리**되었음을 보장해야 한다
- 프로듀서가 초당 100,000개를 발행하는데 컨슈머가 초당 1개밖에 처리 못하면, Push 방식에서는 메시지 유실이 발생한다
- Pull 방식에서는 컨슈머가 처리 가능한 속도로 직접 요청하고, 처리 후 **acknowledgement**를 보내므로 안전하게 처리 가능

## max.poll.records

컨슈머 드라이버가 브로커에 메시지를 요청할 때 **한 번에 가져올 최대 메시지 수**를 제한하는 속성이다.

- 기본값: `500`
- 브로커에 메시지가 1,000,000개 있어도 한 번 요청에 최대 500개만 수신
- 브로커에 메시지가 1개뿐이면 1개만 수신

# Kafka Console Consumer

학습, 테스트, 디버깅 용도의 CLI 메시지 수신 도구. 프로덕션 용도로는 사용하지 않는다.

## 메시지 수신

```bash
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo-topic
```

- 실행 시점 이후에 도착하는 **새 메시지만** 수신 (기존 메시지는 보이지 않음)
- 메시지가 도착하면 즉시 출력, 대기 상태 유지
- `Ctrl + C` 로 종료

### 기본 동작이 새 메시지만 수신하는 이유

애플리케이션마다 필요가 다르기 때문이다. 예를 들어 실시간 날씨 정보나 주가 데이터를 소비하는 애플리케이션은 과거 데이터가 필요 없고 현재 시점부터의 데이터만 필요하다.

## 처음부터 전체 메시지 수신

```bash
./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic demo-topic \
  --from-beginning
```

- 토픽에 저장된 **모든 기존 메시지**를 처음부터 수신
- 이후 새로 도착하는 메시지도 계속 수신

# 다중 컨슈머 동작

컨슈머를 여러 개 실행하면, **모든 컨슈머가 동일한 메시지를 전부 수신**한다.

## 동작이 올바른 경우 — 서로 다른 마이크로서비스

```
order-events → Payment Service   (모든 이벤트 수신)
             → Inventory Service (모든 이벤트 수신)
```

서로 다른 서비스가 동일 토픽을 각각 구독하는 경우, 모든 이벤트를 받아야 하므로 올바른 동작이다.

## 동작이 올바르지 않은 경우 — 동일 서비스의 다중 인스턴스

```
order-events → Payment Service Instance 1 ┐ 동일 메시지 → 중복 처리 발생
             → Payment Service Instance 2 ┘
```

부하 분산을 위해 동일 서비스를 여러 인스턴스로 실행할 때, 모든 인스턴스가 같은 메시지를 받으면 중복 처리가 발생한다. 이를 해결하는 것이 **Consumer Group**이다.

# Consumer Group

## 문제 상황

프로덕션 환경에서는 부하 분산을 위해 동일 서비스를 여러 인스턴스로 실행한다.

```
order-events → Inventory Instance 1 ┐
             → Inventory Instance 2 ├ 모두 동일 메시지 수신 → 중복 처리 발생
             → Inventory Instance 3 ┘
```

원하는 동작: 인스턴스 중 **하나만** 이벤트를 수신하여 처리

## 해결책: Consumer Group

컨슈머 시작 시 `--group` 옵션으로 그룹명을 지정하면, Kafka 브로커가 같은 그룹의 컨슈머들을 **하나의 논리적 컨슈머**로 인식한다.

- **같은 그룹**: 메시지를 인스턴스 간에 분담 처리 (중복 없음)
- **다른 그룹**: 각 그룹이 모든 메시지를 독립적으로 수신

```
order-events ─┬─ group: inventory-service → Instance 1, 2, 3이 메시지 분담
              └─ group: payment-service   → Instance 1, 2가 메시지 분담
```

각 그룹은 서로 다른 서비스를 대표하므로, 그룹 간에는 메시지가 공유되지 않고 각자 전체를 수신한다.

## 데모

### 클린 상태로 Kafka 재시작

```bash
docker compose down
docker compose up
```

```bash
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic demo-topic
```

### 서로 다른 그룹으로 컨슈머 2개 실행

```bash
# 터미널 1 — payment-service 그룹
./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic demo-topic \
  --property print.offset=true \
  --group payment-service

# 터미널 2 — inventory-service 그룹
./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic demo-topic \
  --property print.offset=true \
  --group inventory-service
```

두 컨슈머 모두 모든 메시지를 수신한다 (서로 다른 그룹이므로 독립적).

### 프로듀서 시작

```bash
./kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic demo-topic
```

### 컨슈머 그룹 목록 조회

```bash
./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```