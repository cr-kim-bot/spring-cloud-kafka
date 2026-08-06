# Partition

## 배경: 순서 보장 vs 수평 확장

같은 Consumer Group 내에서 메시지 순서를 보장하려면 하나의 컨슈머만 이벤트를 처리해야 했다 ([[04_kafka_consumer]] 참고). 하지만 이 방식으로는 수평 확장이 불가능하다. Kafka는 **Partition** 개념으로 이 두 요구사항을 동시에 해결한다.

## 토픽과 파티션의 관계

- **토픽(Topic)**: 논리적 추상화 단위
- **파티션(Partition)**: 실제 데이터가 저장되는 물리적 저장 단위
- 하나의 토픽은 여러 개의 파티션으로 구성된다

```
Topic (demo-topic)
 ├─ Partition 0
 └─ Partition 1
```

- 토픽 생성 시 파티션 개수를 지정 가능
- 지정하지 않으면 기본값 **1개 파티션**으로 생성됨 (지금까지의 예제가 이 경우)

## Message Key와 파티션 할당

메시지(이벤트)는 **key**를 가질 수 있다. (DB의 primary key와는 무관한 개념이며 `null`도 가능하다.)

Kafka는 key를 기반으로 어느 파티션에 메시지를 저장할지 결정한다.

```
partition = hash(key) % 파티션 개수
```

- 실제 Kafka 소스코드도 이와 동일한 개념의 로직을 사용
- **같은 key를 가진 메시지는 항상 같은 파티션**으로 전송됨

## 파티션 계산 주체: 클라이언트 라이브러리

파티션 계산은 **Kafka 서버가 아니라 클라이언트 라이브러리(드라이버)**가 수행한다.

```
1. 애플리케이션 → key와 함께 메시지를 client library에 전달
2. client library → hash(key) % 파티션 수 로 파티션 계산
3. client library → "이 메시지는 partition N으로" Kafka 서버에 전달
4. Kafka 서버 → 지정된 파티션에 메시지 저장 (계산에는 관여하지 않음)
```

- 파티션 지정을 client library에 맡기지 않고, 애플리케이션이 **직접 파티션 번호를 지정**하는 것도 가능 (일반적으로는 잘 사용하지 않음)

## 예시: 은행 애플리케이션

계좌번호(account id)를 key로 사용하는 경우:

```
Partition 0: account a1의 deposit, withdraw 이벤트 (순서 보장)
Partition 1: account a2, a7 ... 의 이벤트 (순서 보장)
```

- 계좌 `a1`에 대한 `deposit`과 `withdraw` 이벤트는 항상 같은 파티션에 순서대로 저장됨
- 서로 다른 계좌의 이벤트는 다른 파티션에 분산 가능

## 순서 보장 + 수평 확장 동시 해결

파티션 단위로 컨슈머를 할당하면 각 파티션 내부의 순서는 유지하면서 병렬 처리가 가능하다.

```
Partition 0 → Consumer Instance 1  (a1 이벤트를 순서대로 처리)
Partition 1 → Consumer Instance 2  (a2, a7 이벤트를 순서대로 처리)
```

- 같은 계좌의 이벤트는 항상 같은 파티션에 있으므로 `withdraw`가 `deposit`보다 먼저 처리되는 문제가 발생하지 않음
- 파티션이 여러 개이므로 여러 컨슈머 인스턴스가 병렬로 처리 가능

## Offset은 파티션 단위

Offset은 토픽이 아니라 **파티션 단위**로 관리된다.

```
Partition 0: offset 0, 1, 2, 3 ...
Partition 1: offset 0, 1, 2, 3 ...
```

- 파티션마다 독립적인 offset 시퀀스를 가짐
- 파티션 간 메시지 개수가 달라 offset 진행 속도도 다를 수 있음 (정상적인 현상)

## Key 선택의 중요성

파티션 분산이 고르게 이루어지려면 **key를 신중하게 선택**해야 한다.

**잘못된 예**: 현재 날짜를 key로 사용

- 하루 동안 발행된 수백만 건의 메시지가 모두 같은 key → 전부 같은 파티션으로 몰림 → 분산 효과 없음

**올바른 예**: 도메인에 맞는 식별자를 key로 사용

| 이벤트 종류 | 권장 key |
|---|---|
| 계좌 관련 이벤트 | account id |
| 사용자 클릭 이벤트 | user id |
| 주문 이벤트 | order id |

> 값이 다양하게 분포하는 필드를 key로 선택해야 파티션 간 부하가 고르게 분산된다.

## 파티션과 리더/팔로워의 관계 재정리

[[01_kafka_core_fundarmentals]]에서 리더/팔로워를 "토픽 단위"로 설명했지만, 정확히는 **파티션 단위**로 리더/팔로워가 지정된다.

- 파티션이 1개인 토픽 → 해당 파티션의 리더/팔로워가 곧 토픽의 리더/팔로워처럼 보임 (지금까지의 단순화된 설명)
- 파티션이 여러 개인 토픽 → 각 파티션마다 리더가 다른 브로커 노드일 수 있음

```
order-events (토픽, 논리적 구성)
 ├─ Partition 0 → Leader: Node A, Follower: Node B
 └─ Partition 1 → Leader: Node B, Follower: Node A
```

- **토픽 = 논리적 구성**, **파티션 = 물리적 구성**
- 컨트롤러가 파티션들을 여러 브로커 노드에 분산 배치
- 하나의 노드가 파티션 A의 리더이면서 동시에 파티션 B의 팔로워일 수 있음

## 데모: 2개 파티션으로 토픽 생성

### 클린 상태로 Kafka 재시작

```bash
docker compose down
docker compose up
```

### 파티션 2개로 토픽 생성

```bash
./kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --topic order-events \
    --create \
    --partitions 2
```

### 토픽 상세 조회

```bash
./kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --topic order-events \
    --describe
```

출력 결과에서 `Partition: 0`, `Partition: 1`이 각각 표시되며, 각 파티션의 `Leader` 필드에 브로커의 **node ID**가 나온다.

### node ID 확인

```bash
grep node.id /opt/kafka/config/server.properties
```

- `server.properties`에 설정된 `node.id`가 클러스터 내 해당 브로커의 고유 식별자
- 단일 노드(standalone) Docker 환경에서는 `node.id=1`이 모든 파티션의 리더로 표시됨 (파티션을 분산시킬 다른 노드가 없기 때문)
- 실제 다중 노드 클러스터에서는 파티션별로 서로 다른 node ID가 리더로 표시됨

## 데모: Consumer Group과 파티션 분배

**목표**: 파티션 2개짜리 토픽 + 같은 그룹의 컨슈머 2개 → Kafka가 파티션을 컨슈머에 1:1로 분배하는지 확인

### 사전 조건

- 위 데모에서 생성한 `order-events` 토픽(파티션 2개)이 존재해야 함

### 같은 그룹의 컨슈머 2개 실행

```bash
# 터미널 1
docker exec -it kafka bash

./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --property print.offset=true \
  --property print.key=true \
  --group payment-service
```

```bash
# 터미널 2
docker exec -it kafka bash

./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --property print.offset=true \
  --property print.key=true \
  --group payment-service
```

- 두 컨슈머 모두 `payment-service` 그룹에 속함
- `print.key=true`로 메시지의 key도 함께 출력

### key를 포함한 프로듀서 실행

```bash
# 터미널 3
docker exec -it kafka bash

./kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --property parse.key=true \
  --property key.separator=:
```

- `parse.key=true` + `key.separator=:` → 입력을 `key:value` 형식으로 해석
- 예: `1:A` 입력 시 key=`1`, value=`A`로 전송

### 관찰 결과

```
1:A  →  key=1, hash(1)%2=partition 0  →  Consumer 1이 수신
2:B  →  key=2, hash(2)%2=partition 1  →  Consumer 2가 수신
3:C  →  key=3, hash(3)%2=partition 0  →  Consumer 1이 수신
1:D  →  key=1  → 항상 partition 0     →  Consumer 1이 수신
2:E  →  key=2  → 항상 partition 1     →  Consumer 2가 수신
```

- 각 파티션은 그룹 내 **정확히 하나의 컨슈머**에게만 할당됨
- 같은 key의 메시지는 항상 같은 파티션 → 같은 컨슈머가 순서대로 수신
- 파티션(2개) + 컨슈머 그룹(2개 인스턴스) 조합으로 **부하 분산과 메시지 순서 보장을 동시에 달성**

# Partition Rebalancing

Consumer Group 내에서 **컨슈머 수가 변할 때마다** Kafka는 파티션을 다시 분배한다. 이를 **partition rebalancing**이라 한다.

## 발생 시점

- 그룹에 **새 컨슈머가 합류**할 때 (예: 오토스케일링으로 인스턴스 증가)
- 그룹에서 **컨슈머가 이탈**할 때 (크래시, 네트워크 장애 등)

## 동작 예시 (파티션 2개, `payment-service` 그룹)

**1) 컨슈머 1개일 때**

```
Consumer 1 ← Partition 0, Partition 1  (모두 할당)
```

컨슈머가 하나뿐이면 Kafka는 모든 파티션을 그 컨슈머에게 할당한다.

**2) 컨슈머 2번째가 합류 → rebalancing 발생**

```
Consumer 1 ← Partition 0
Consumer 2 ← Partition 1
```

Kafka가 새 컨슈머의 합류를 감지하고 파티션을 재분배하여 각 컨슈머에 하나씩 할당한다.

**3) 컨슈머 중 하나가 이탈(크래시 등) → rebalancing 재발생**

```
Consumer 1 ← Partition 0, Partition 1  (남은 컨슈머가 전부 인수)
```

Kafka가 컨슈머 손실을 감지하고, 남은 컨슈머에게 유실된 파티션을 재할당한다.

## 핵심 정리

- 파티션 할당은 고정되어 있지 않고 **그룹 멤버십 변화에 따라 동적으로 재조정**됨
- 컨슈머 수가 늘어나면 파티션이 더 잘게 분산되고, 줄어들면 남은 컨슈머가 더 많은 파티션을 담당
- 그룹 내 컨슈머 수는 **파티션 수를 넘지 않는 것이 효율적** — 파티션보다 컨슈머가 많으면 일부 컨슈머는 할당받을 파티션이 없어 유휴 상태가 됨

## 시나리오: 파티션 3개, 컨슈머가 순차적으로 증가하는 경우

`payment-service` 그룹, 파티션 3개(`order-events`) 기준.

| 컨슈머 수 | 파티션 할당 |
|---|---|
| 1개 | Consumer 1 ← Partition 0, 1, 2 (전부) |
| 2개 (오토스케일링) | Consumer 1 ← Partition 0, 1 / Consumer 2 ← Partition 2 |
| 3개 (오토스케일링) | Consumer 1 ← P0 / Consumer 2 ← P1 / Consumer 3 ← P2 (1:1 매핑) |
| 4개 (오토스케일링) | 위와 동일 + **Consumer 4는 유휴(idle) 상태** |

## 컨슈머 수 > 파티션 수인 경우

- **하나의 파티션은 절대 여러 컨슈머에게 분할되지 않는다** — 메시지 순서 보장 원칙 때문
- 파티션 개수보다 컨슈머가 많으면, 초과된 컨슈머는 할당받을 파티션이 없어 **유휴 상태로 대기**
- 반대로 하나의 컨슈머가 여러 파티션을 담당하는 것은 문제 없음

> 따라서 Consumer Group의 최대 병렬 처리 수는 **파티션 개수**로 제한된다. 확장하려면 파티션 수를 늘려야 한다.

# 파티션 수 증가와 메시지 순서 문제

## 파티션 개수 늘리기

```bash
./kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --alter \
  --partitions 4
```

- `--alter` 옵션으로 파티션 개수 **증가만 가능**, 감소는 불가능

## 문제: 기존 key의 파티션 할당이 바뀐다

파티션 계산식이 `hash(key) % 파티션 개수`이므로, 파티션 개수가 바뀌면 **같은 key라도 다른 파티션으로 매핑**된다.

```
파티션 3개일 때: key=4 → partition 1
파티션 4개일 때: key=4 → partition 3   (매핑이 바뀜)
```

- 기존에 쌓여 있던 메시지는 이전 파티션에 그대로 남아있고, 새로 발행되는 같은 key의 메시지는 다른 파티션으로 감
- 그 결과 새 메시지가 예전 메시지보다 먼저 소비되는 경우가 생겨 **일시적으로 메시지 순서 보장이 깨질 수 있음**

## 해결 방법

### 1. 사전 설계로 예방

- 처음부터 향후 3~5년의 트래픽 증가를 고려하여 **여유 있는 파티션 개수**로 토픽 생성
- 파티션이 컨슈머 수보다 많은 것은 문제 없음 (초과분은 유휴 상태일 뿐)
- 단, 무조건 많이 만드는 것도 권장하지 않음 (별도 Best Practice에서 다룸)

### 2. 순서 문제를 허용 가능한 경우 그대로 진행

- 모든 토픽이 미션 크리티컬한 것은 아님
- 예: "고객이 iPhone을 조회 → iPad를 조회"처럼 순서가 일시적으로 바뀌어도 비즈니스에 큰 영향이 없는 경우 그대로 `--alter` 진행 가능

### 3. 프로듀서 중지 후 파티션 증가 (다운타임 발생)

1. 프로듀서 중지
2. `--alter`로 파티션 수 증가
3. 컨슈머가 기존 파티션의 메시지를 모두 소비할 때까지 대기 (drain)
4. 모든 메시지 소비 완료 후 프로듀서 재시작

- 순서 문제는 없지만 **일시적인 다운타임** 발생

### 4. 새 토픽으로 전환 (다운타임 없음, 권장)

토픽은 애플리케이션 코드가 아니라 **설정값**이므로, 파티션 수를 바꾸는 대신 새 토픽을 만들어 전환한다.

1. 파티션 4개로 새 토픽 생성 (예: `order-events-v2`)
2. 프로듀서의 `application.properties`에서 토픽명을 새 토픽으로 변경 후 재시작 → 신규 메시지는 새 토픽으로 발행
3. 컨슈머는 계속 기존 토픽(`order-events`)에서 메시지를 소비하여 drain
4. 기존 토픽의 메시지를 모두 소비하면, 컨슈머의 `application.properties`도 새 토픽으로 변경 후 재시작

- 코드 변경 없이 설정만 변경, 다운타임 없이 전환 가능

# Null Key 데모

**목표**: key 없이(null key) 메시지를 2파티션 토픽에 전송할 때 Kafka가 어떻게 분배하는지 관찰

## 토픽 생성

```bash
docker exec -it kafka bash

./kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --topic null-key-topic \
  --create \
  --partitions 2
```

## 같은 그룹의 컨슈머 2개 실행

```bash
# 터미널 1, 2 동일 명령
docker exec -it kafka bash

./kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic null-key-topic \
  --property print.offset=true \
  --property print.key=true \
  --group null-key-group
```

## 프로듀서 실행 (50ms마다 key 없이 발행)

```bash
i=1
while true; do
  echo "msg$i"
  i=$((i+1))
  sleep 0.05
done | ./kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic null-key-topic \
  --timeout 0
```

## 관찰 결과

- key가 있을 때처럼 메시지마다 **round robin으로 파티션을 번갈아 분배하지 않는다**
- 한동안 Consumer 1이 메시지를 계속 받고, Consumer 2는 유휴 상태로 대기
- 이후 어느 시점에 Kafka가 파티션을 전환하여 Consumer 2가 메시지를 받기 시작

```
Consumer 1: msg1, msg2, msg3 ... msgN   (일정 기간 몰림)
Consumer 2: (유휴) ... msgN+1, msgN+2 ...  (이후 전환)
```

> null key 사용 시 파티션은 **메시지 단위가 아닌 배치(batch) 단위로 sticky하게 할당**되어, 짧은 시간 동안은 한 파티션에 메시지가 몰리는 것처럼 보인다.