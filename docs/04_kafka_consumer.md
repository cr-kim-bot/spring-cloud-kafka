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

## 익명 Consumer Group

Kafka의 모든 컨슈머는 반드시 컨슈머 그룹에 속한다. `--group`을 지정하지 않으면 Kafka가 **랜덤 그룹명을 자동 할당**한다.

- 그룹명 없이 실행한 컨슈머 2개 → 각각 다른 랜덤 그룹에 배정 → 두 컨슈머 모두 전체 메시지 수신
- 이것이 이전 데모에서 그룹 미지정 시 양쪽 컨슈머가 같은 메시지를 받은 이유

비활성 그룹은 기본적으로 **7일 후 자동 삭제**된다 (`offsets.retention.minutes` 설정으로 조정 가능).

# 메시지 순서 보장과 수평 확장의 트레이드오프

## 동일 그룹 내 다중 인스턴스에서 한 인스턴스만 받는 이유

Kafka는 **메시지 순서 보장**을 핵심 원칙으로 한다. 메시지를 인스턴스에 무작위로 분산하면 순서가 깨져 심각한 문제가 생긴다.

**예시 — 은행 애플리케이션**

1. 고객이 $1,000 입금 → `deposit $1000` 이벤트 발행
2. 고객이 즉시 $50 출금 → `withdraw $50` 이벤트 발행

만약 Kafka가 두 이벤트를 서로 다른 인스턴스에 분산하면:

```
Instance 1: deposit $1000 처리 중 (10초 소요)
Instance 2: withdraw $50 처리 시작 → 잔액 없음 예외 발생!
```

입금이 완료되기 전에 출금이 먼저 처리되어 오류가 발생한다. **이벤트는 반드시 발생 순서대로 처리되어야 한다.**

## 수평 확장 문제

- 순서 보장을 위해 단일 컨슈머만 사용하면 수평 확장이 불가능해 보인다
- 하지만 Kafka는 **Partition** 개념으로 이 문제를 해결한다 (이후 강의에서 설명)

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

# Offset Tracking

Consumer Group이 토픽을 처음 구독할 때부터, Kafka는 내부적으로 **어느 그룹이 어느 파티션의 offset까지 읽었는지** 추적한다.

> "ledger"는 공식 Kafka 용어가 아니라 이해를 돕기 위한 비유적 표현이다.

```
Partition 0: 전체 메시지 offset 0~10
  └─ payment-service 그룹: offset 3까지 전달 완료 (추적 중)
```

## 필요성

마이크로서비스 환경에서는 컨슈머 프로세스가 예기치 않게 종료될 수 있다 (OOM, 네트워크 장애 등).

- 컨슈머가 크래시 후 재시작하여 그룹에 다시 합류
- 이때 Kafka가 offset을 추적하지 않는다면, 메시지를 처음부터(offset 0) 다시 전달해야 함 → 중복 처리 발생
- Kafka는 "이 그룹에게 offset 3까지 전달했다"를 기억하므로, 재시작 후 **offset 4부터 이어서 전달**

## 추적 단위

Offset tracking은 **Consumer Group × Partition** 조합마다 개별적으로 이루어진다.

- 같은 파티션이라도 그룹이 다르면 각자 독립적인 offset을 추적
- 같은 그룹이라도 파티션이 다르면 각자 독립적인 offset을 추적

## 데모: Offset Tracking 확인

### 파티션 2개로 토픽 생성

```bash
docker exec -it kafka bash

./kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --topic offset-tracking-topic \
  --create \
  --partitions 2
```

### 컨슈머 없이 먼저 메시지 5개 생성

기존에 데이터가 쌓여있는 토픽을 시뮬레이션하기 위해, 컨슈머 그룹을 만들기 **전에** key와 함께 메시지 5개를 먼저 발행한다.

```bash
./kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic offset-tracking-topic \
  --property parse.key=true \
  --property key.separator=:
```

```
1:a
2:a
3:a
4:a
5:a
```

이 시점에는 아직 컨슈머 그룹이 없으므로 Kafka가 추적할 대상도 없다.

### 컨슈머 그룹 시작 (from-beginning 사용 안 함)

```bash
# 터미널 1, 2 동일 명령
./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic offset-tracking-topic \
  --property print.offset=true \
  --property print.key=true \
  --group cg
```

- `--from-beginning`을 의도적으로 사용하지 않음 → "이 시점 이후의 새 메시지만 달라"는 의미
- Kafka는 이 그룹이 **기존 메시지 5개를 이미 다 본 것으로 간주**하고 현재 offset부터 추적 시작

### Consumer Group 상태 조회

```bash
./kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group cg
```

| 컬럼 | 의미 |
|---|---|
| `LOG-END-OFFSET` | 해당 파티션에 저장된 **최신 offset** (Kafka에 존재하는 전체 데이터 양) |
| `CURRENT-OFFSET` | 해당 컨슈머 그룹이 **마지막으로 커밋한 offset** (그룹이 처리한 지점) |
| `LAG` | `LOG-END-OFFSET − CURRENT-OFFSET`, 컨슈머가 프로듀서보다 얼마나 뒤처졌는지 |

```
LAG = LOG-END-OFFSET − CURRENT-OFFSET
```

**초기 상태**: `--from-beginning` 없이 그룹이 막 합류했으므로 `CURRENT-OFFSET == LOG-END-OFFSET`, `LAG = 0` (기존 5개 메시지는 "이미 본 것"으로 처리됨)

### 시나리오 1 — 컨슈머 실행 중 메시지 추가 생산

같은 방식으로 메시지 5개(`1A~5A`)를 추가 발행하면, 실행 중인 두 컨슈머가 즉시 나눠서 수신한다.

- `LOG-END-OFFSET`과 `CURRENT-OFFSET`이 함께 증가 → `LAG = 0` 유지 (실시간으로 처리되므로)

### 시나리오 2 — 컨슈머 종료 후 메시지 추가 생산

두 컨슈머를 모두 종료(`Ctrl+C`)한 뒤, 메시지 5개(`1B~5B`)를 추가 발행하고 다시 `--describe` 조회:

```
partition 0: LOG-END-OFFSET=6, CURRENT-OFFSET=6 → LAG=0  (컨슈머 종료 전과 동일)
partition 1: LOG-END-OFFSET=9, CURRENT-OFFSET=6 → LAG=3
```

- 컨슈머가 없는 동안 발행된 메시지만큼 `LOG-END-OFFSET`은 증가하지만 `CURRENT-OFFSET`은 그대로 → `LAG` 발생
- `--describe` 결과에 그룹의 활성 멤버가 없다고(no active members) 표시됨

### 컨슈머 재시작 → Lag 해소

컨슈머 중 하나만 다시 시작해도, Kafka는 **해당 그룹의 모든 파티션**을 그 컨슈머에게 즉시 할당한다 (다른 컨슈머를 기다리지 않음).

- 밀려있던 메시지(`1B~5B`)가 즉시 전달됨
- 이후 두 번째 컨슈머가 합류하면 partition rebalancing이 다시 발생
- 최종적으로 `CURRENT-OFFSET == LOG-END-OFFSET`, `LAG = 0`으로 정상화

## `--from-beginning`은 최초 1회만 유효

`--from-beginning`(또는 latest, 기본값)은 **해당 Consumer Group이 그 파티션에 대한 offset 추적을 처음 시작할 때만** 적용된다.

- 최초 구독 시점에 offset 추적 항목(entry)이 아직 없으므로, 이때만 "처음부터 줄까, 최신부터 줄까"를 결정
- 한 번 `CURRENT-OFFSET`이 기록되고 나면, 이후에는 **항상 커밋된 offset부터 이어서 전달**
- 이미 추적 중인 그룹이 나중에 `--from-beginning`을 붙여서 재접속해도 무시되고 커밋된 offset부터 전달됨

> 위 데모에서 컨슈머가 처음 `--from-beginning` 없이 접속했을 때 기존 메시지 5개(`CURRENT-OFFSET`)를 건너뛴 것도 이 규칙 때문이다. 한 번 offset이 3, 2로 기록된 이후에는 어떤 옵션을 붙여도 그 지점부터만 이어진다.

# Offset Reset

`LAG = 0`인 상태(모든 메시지 전달 완료)라면 Kafka는 같은 메시지를 재전달하지 않는다. 하지만 애플리케이션 버그 수정 후 과거 메시지를 다시 처리하고 싶은 경우처럼, **의도적으로 재전달을 받고 싶을 때** 컨슈머 그룹의 커밋된 offset을 재설정할 수 있다.

## Reset 옵션

| 옵션 | 의미 |
|---|---|
| `--shift-by <N>` (양수) | 현재 offset에서 **N만큼 앞으로** 이동 (예: 5 → 8) |
| `--shift-by <-N>` (음수) | 현재 offset에서 **N만큼 뒤로** 이동 → 그만큼 재전달 |
| `--by-duration <PT5M>` | 최근 N 기간 동안의 메시지를 재전달 |
| `--to-datetime <날짜시간>` | 지정한 시각 이후의 메시지를 재전달 |
| `--to-earliest` | offset을 0으로 → 처음부터 전체 재전달 |
| `--to-latest` | offset을 최신으로 → lag을 0으로 초기화 (새 메시지만 수신) |

> reset은 특정 파티션 하나만 지정할 수 없고 **토픽의 모든 파티션에 동일하게 적용**된다.

## Dry Run vs Execute

| 모드 | 동작 |
|---|---|
| `--dry-run` | 실제로 offset을 바꾸지 않고, 적용될 새 offset 값만 미리 보여줌 |
| `--execute` | 실제로 offset을 재설정함 |

## 명령어 형식

```bash
./kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --topic offset-tracking-topic \
  --group cg \
  --reset-offsets \
  --shift-by -3 \
  --dry-run    # 또는 --execute
```

> reset 전에 **해당 그룹의 컨슈머를 모두 중지**해야 한다. 컨슈머가 실행 중이면 Kafka가 offset을 재설정할 수 없다.

## 데모: shift-by -3 으로 재전달

위 Offset Tracking 데모에 이어서 진행 (파티션 2개 토픽 + `cg` 그룹 필요).

**시작 상태**: `CURRENT-OFFSET` = partition 0: 6, partition 1: 9

### Dry Run으로 미리 확인

```bash
./kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --topic offset-tracking-topic \
  --group cg \
  --reset-offsets \
  --shift-by -3 \
  --dry-run
```

결과: 새 offset이 partition 0: 6, partition 1: 3으로 표시됨 (아직 적용 안 됨)

### Execute로 실제 적용

```bash
./kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --topic offset-tracking-topic \
  --group cg \
  --reset-offsets \
  --shift-by -3 \
  --execute
```

- `--describe` 조회 시 `CURRENT-OFFSET`이 실제로 6, 3으로 변경되고 `LAG`이 3, 3으로 표시됨

### 컨슈머 재시작

- 컨슈머를 다시 시작하면 rollback된 지점부터 메시지가 재전달됨 (최근 6개 메시지 재수신)
- 재전달이 끝나면 다시 `LAG = 0`으로 정상화