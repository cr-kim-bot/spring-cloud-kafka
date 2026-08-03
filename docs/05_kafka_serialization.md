# 직렬화 / 역직렬화 (Serialization / Deserialization)

Kafka는 내부적으로 모든 메시지를 **바이트(byte[])**로 저장하고 전송한다. 메시지의 내용을 이해하거나 해석하지 않는다.

```
Producer App  →  직렬화(byte[])  →  Kafka Broker  →  역직렬화  →  Consumer App
```

- **직렬화**: 프로듀서가 메시지를 byte[]로 변환하여 브로커에 전달
- **역직렬화**: 컨슈머가 브로커로부터 받은 byte[]를 다시 객체로 변환
- 직렬화/역직렬화는 브로커가 아닌 **각 클라이언트 애플리케이션**이 담당

## Serializer / Deserializer 설정

Kafka Client Library에 기본 Serializer/Deserializer가 내장되어 있다 (Integer, String 등).

복잡한 Java 객체(OrderEvent, PaymentEvent 등)를 전송할 때는 **Jackson 라이브러리**를 사용한다.

```
Java Object  →  Jackson  →  JSON String  →  byte[]  →  Kafka Broker
```

Spring Framework가 이 직렬화 과정을 자동으로 처리해준다.

## Console Producer / Consumer의 직렬화

Console Producer와 Console Consumer는 기본적으로 **String** 타입으로 처리한다.

| 도구 | 동작 |
|---|---|
| Console Producer | 입력 문자열 → byte[] 변환 후 전송 |
| Console Consumer | byte[] 수신 → 문자열로 변환 후 출력 |