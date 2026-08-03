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