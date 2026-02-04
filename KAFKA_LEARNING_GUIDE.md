# Kafka 속성 교육 - 기초부터 실무까지 완벽 가이드

내일 면접을 위한 Kafka 속성 교육 자료입니다. 각 개념을 예제 코드와 함께 학습하세요.

## 📚 학습 로드맵

```
Day 1: 기초 개념 (2시간)
├─ 1. Kafka의 기본 아키텍처
├─ 2. Producer 구현 (동기/비동기)
├─ 3. Consumer 구현 (단일/배치)
└─ 4. 파티셔닝과 순서 보장

Day 1-2: 실무 기술 (3시간)
├─ 5. 에러 처리 및 재시도
├─ 6. 트랜잭션과 Exactly-once
├─ 7. 메시지 헤더와 메타데이터
└─ 8. 모니터링 및 성능 최적화

Day 2: 고급 패턴 (2시간)
├─ 9. Event Sourcing 패턴
├─ 10. CQRS 패턴
└─ 11. 실무 사례 및 트러블슈팅
```

---

## 1️⃣ Kafka의 기본 아키텍처

### Kafka란?
- **분산 메시지 큐** 시스템
- **높은 처리량**: 초당 수백만 메시지 처리
- **낮은 지연**: 밀리초 단위 지연
- **영속성**: 메시지를 디스크에 저장
- **확장성**: Broker 추가로 쉽게 확장

### 핵심 개념

#### 1.1 Topic (토픽)
- **정의**: 메시지의 범주 (예: order-events, payment-events)
- **특징**: 여러 Partition으로 나뉨
- **용도**: 특정 종류의 이벤트 발행

**예제**:
```
Topic: order-events
├─ Partition 0: (key=order-001) → (key=order-003) → (key=order-005)
├─ Partition 1: (key=order-002) → (key=order-004) → (key=order-006)
└─ Partition 2: (key=order-007) → (key=order-008) → (key=order-009)
```

#### 1.2 Partition (파티션)
- **정의**: Topic의 분할된 부분
- **특징**: 각 Partition은 독립적인 메시지 큐
- **순서**: 같은 Partition 내에서는 순서 보장
- **병렬성**: 여러 Partition을 동시에 처리

**파티셔닝 규칙**:
```
MessageKey: order-001
Hash(order-001) % PartitonCount = 1
→ Partition 1에 저장
```

#### 1.3 Broker (브로커)
- **정의**: Kafka 서버
- **역할**: 메시지 저장, Consumer 요청 처리
- **복제**: 각 Partition은 여러 Broker에 복제됨

**예제 - Replication Factor = 3**:
```
Topic: order-events, Partition 0
├─ Leader (Broker 1)       - 모든 읽기/쓰기 처리
├─ Replica (Broker 2)      - 동기화됨 (장애 시 인수)
└─ Replica (Broker 3)      - 동기화됨 (장애 시 인수)
```

#### 1.4 Consumer Group (컨슈머 그룹)
- **정의**: 같은 Topic을 구독하는 Consumer들의 모임
- **특징**: Partition은 Group 내의 한 Consumer에게만 할당됨
- **로드 밸런싱**: Consumer 추가/제거 시 자동 재분배

**예제 - Consumer Group A (3명)**:
```
Topic: order-events (3개 Partition)
├─ Consumer 1 → Partition 0
├─ Consumer 2 → Partition 1
└─ Consumer 3 → Partition 2

결과: 3개 Consumer가 병렬로 메시지 처리
```

**예제 - Consumer Group B (1명)**:
```
Topic: order-events (3개 Partition)
└─ Consumer 1 → Partition 0, 1, 2 (모두 처리)

결과: 1개 Consumer가 모든 Partition 처리
```

#### 1.5 Offset (오프셋)
- **정의**: Partition 내 메시지의 위치
- **특징**: 0부터 시작하는 순차 번호
- **용도**: Consumer가 처리한 위치 추적

**예제**:
```
Partition 0:
Offset 0: order-001 | created_at: 10:00:00
Offset 1: order-002 | created_at: 10:00:01
Offset 2: order-003 | created_at: 10:00:02
Offset 3: order-004 | created_at: 10:00:03

Consumer의 현재 위치 (Committed Offset): 2
→ 다음에는 Offset 3부터 읽음
```

---

## 2️⃣ Producer 구현

### 2.1 동기 전송 (Synchronous)

**코드**:
```kotlin
val result = kafkaTemplate.sendDefault("order-events", orderId, event).get()
//                                                                       ↑
//                                                               .get() = 동기 대기
```

**특징**:
- ✅ 안정적: 성공 여부 즉시 확인
- ✅ 순서 보장: 같은 Key면 순서 유지
- ❌ 느림: 대기 시간 발생
- ❌ 낮은 처리량: 한 번에 하나씩

**사용 시기**:
- 결제, 금융 거래 (정확성 중요)
- 법적 증거 필요
- 즉시 응답 필요

### 2.2 비동기 전송 (Asynchronous)

**코드**:
```kotlin
kafkaTemplate.send("payment-events", orderId, event)
    .whenComplete { result, exception ->
        // 나중에 처리
    }
```

**특징**:
- ✅ 빠름: 블로킹 없음
- ✅ 높은 처리량: 동시 전송
- ❌ 복잡함: 콜백 처리 필요
- ❌ 순서 보장 어려움

**사용 시기**:
- 로깅, 분석
- 손실 용인 가능
- 높은 처리량 필요

### 2.3 파티셔닝 전략

**규칙**:
```
Partition = Hash(Key) % PartitionCount
```

**파티셔닝 Key 선택**:
```
orderId      → 같은 주문의 모든 이벤트는 같은 Partition (순서 보장)
customerId   → 같은 고객의 모든 주문은 같은 Partition
null         → 라운드 로빈 (순서 미보장)
```

**성능 최적화**:
```
- batch-size: 16KB     (배치 크기)
- linger-ms: 10ms      (배치 대기 시간)
- compression: snappy  (압축)
```

**배치 동작**:
```
Message 1 (2KB)  →┐
Message 2 (3KB)  →├→ 배치 (5KB) → 전송
Message 3 (4KB)  →┘
Message 4 (6KB)  →┐
Message 5 (5KB)  →├→ 배치 (11KB) → 전송
                     ↓
                 10ms 경과 → 배치가 안 찼어도 전송
```

---

## 3️⃣ Consumer 구현

### 3.1 기본 Consumer

**코드**:
```kotlin
@KafkaListener(
    topics = ["order-events"],
    groupId = "order-service-group"
)
fun listen(event: OrderCreatedEvent) {
    // 메시지 처리
}
```

**특징**:
- 자동 커밋: `enable-auto-commit: true`
- 병렬 처리: `concurrency: 3`
- 간단함

**주의**:
```
메시지 처리 중 장애 → 자동 커밋 전 충돌 → 중복 처리 가능
```

### 3.2 배치 Consumer

**코드**:
```kotlin
@KafkaListener(topics = ["payment-events"], groupId = "payment-group")
fun listenBatch(events: List<PaymentCompletedEvent>) {
    // 배치 처리: 여러 메시지를 한 번에 처리
}
```

**성능**:
```
단일 메시지: 메시지 1개당 1ms → 총 1000ms (1000개)
배치 처리: 배치당 5ms × 10배치 → 총 50ms (1000개)
          성능 20배 향상! 🚀
```

### 3.3 에러 처리 및 재시도

**구조**:
```
shipping-events (원본)
    ↓ (실패)
shipping-events-retry-0 (1초 지연 후 재시도)
    ↓ (실패)
shipping-events-retry-1 (2초 지연 후 재시도)
    ↓ (실패)
shipping-events-dlt (Dead Letter Topic - 최종 실패)
```

**코드**:
```kotlin
@RetryableTopic(
    attempts = "4",               // 초기 + 3회 재시도
    backoff = Backoff(
        delay = 1000,             // 1초
        multiplier = 2.0          // 2배씩 증가
    )
)
@KafkaListener(topics = ["shipping-events"])
fun listen(event: ShippingPreparedEvent) {
    // 처리 로직
}
```

**지연 전략**:
```
Fixed Backoff: 1s → 1s → 1s → 1s (일정)
Exponential:   1s → 2s → 4s → 8s (지수 증가)
```

### 3.4 수동 커밋

**코드**:
```kotlin
@KafkaListener(topics = ["critical-events"])
fun listen(event: OrderCreatedEvent, acknowledgment: Acknowledgment?) {
    try {
        processEvent(event)
        acknowledgment?.acknowledge()  // 성공 시에만 커밋
    } catch (e: Exception) {
        // 커밋 안 함 → Offset 유지 → 다음 poll에서 재처리
        throw e
    }
}
```

**Exactly-once 보장**:
```
1. 메시지 처리
2. ✓ 성공 → 커밋 → Offset 진행
3. ✗ 실패 → 커밋 안 함 → 다음 폴링에서 같은 메시지 재처리
```

---

## 4️⃣ 실무 기술

### 4.1 트랜잭션과 Exactly-once

**문제**: 중복 처리 방지

**해결책**:
```kotlin
// 1. Idempotent Producer
enable.idempotence: true  // 중복 제거

// 2. Exactly-once Consumer
@KafkaListener(...)
fun listen(event: OrderCreatedEvent, ack: Acknowledgment?) {
    processEvent(event)        // DB 업데이트
    ack?.acknowledge()         // Offset 커밋
    // 이 사이에 장애 → DB는 업데이트, Offset은 미커밋 → 재처리는 멱등적으로 처리
}
```

**멱등성 (Idempotency)**:
```
첫 번째 실행: UPDATE orders SET status = 'COMPLETED' WHERE id = '001'
두 번째 실행: UPDATE orders SET status = 'COMPLETED' WHERE id = '001'
결과: 동일 ✓
```

### 4.2 메시지 헤더와 메타데이터

**목적**: 추적 가능성 (Traceability)

**헤더**:
```
메시지 헤더:
├─ eventId: 고유 이벤트 ID
├─ correlationId: 요청 추적 ID
├─ source: 발행 시스템
└─ timestamp: 발행 시간

메시지 바디:
└─ 실제 데이터 (OrderCreatedEvent)
```

**사용 예**:
```
사용자 요청 → [API] → eventId=abc → [Service A] → 메시지 발행
                                        ↓
                                   [Service B] 구독
                                   같은 eventId 추적
                                        ↓
                                   [Service C] 구독
                                   모두 같은 eventId = 추적 가능!
```

### 4.3 모니터링 지표

**주요 지표**:
```
1. Consumer Lag: 처리하지 못한 메시지 개수
   Lag = LogEndOffset - CommittedOffset
   
   예: LogEndOffset = 1000, CommittedOffset = 850
   → Lag = 150 (150개 미처리)

2. 처리 시간: 메시지당 평균 처리 시간
   예: 1000ms / 100개 = 10ms per message

3. 에러율: 처리 실패 비율
   예: 5개 실패 / 1000개 = 0.5% 에러율
```

---

## 5️⃣ Event Sourcing 패턴

### 5.1 핵심 아이디어

**전통적 방식** (상태 저장):
```
Order Table:
┌─────────┬──────────┬─────────┐
│ id      │ status   │ updated │
├─────────┼──────────┼─────────┤
│ order-1 │ SHIPPED  │ 10:00   │  ← 현재 상태만 저장
└─────────┴──────────┴─────────┘

문제점:
- 변경 이력을 알 수 없음
- "언제" "누가" "왜" 변경했는지 알 수 없음
```

**Event Sourcing** (이벤트 저장):
```
Event Store:
┌───┬──────────┬─────────────────┬────────┐
│ # │ eventId  │ eventType       │ time   │
├───┼──────────┼─────────────────┼────────┤
│ 1 │ evt-001  │ OrderCreated    │ 10:00  │
│ 2 │ evt-002  │ PaymentReceived │ 10:05  │
│ 3 │ evt-003  │ ShippingReady   │ 10:10  │
│ 4 │ evt-004  │ Shipped         │ 10:15  │
└───┴──────────┴─────────────────┴────────┘

장점:
- 완전한 이력 기록
- 과거 특정 시점의 상태 재현 가능 ← 시간 여행!
- 감시 기록 자동 제공
```

### 5.2 상태 재구성

**동작**:
```
현재 상태 = 모든 이벤트를 처음부터 끝까지 순서대로 적용

Event 1: OrderCreated
→ State: { id: order-1, status: CREATED }

Event 2: PaymentReceived
→ State: { id: order-1, status: PAYMENT_COMPLETED }

Event 3: ShippingReady
→ State: { id: order-1, status: SHIPPING_PREPARED }

Event 4: Shipped
→ State: { id: order-1, status: SHIPPED }  ← 현재 상태
```

**시간 여행**:
```
"10:10에서 상태를 알고 싶어"
→ Event 1, 2, 3만 적용
→ State: { id: order-1, status: SHIPPING_PREPARED }
```

### 5.3 성능 최적화: Snapshot

**문제**: 이벤트가 많으면 상태 재구성에 시간 소요
```
1,000,000개 이벤트 = 1초 이상
```

**해결책**: Snapshot (특정 시점의 상태 저장)
```
매 1,000개 이벤트마다 스냅샷 생성:

Snapshot (at Event 1000):
{ id: order-1, status: PAYMENT_COMPLETED }

이후 이벤트 (1001~1003):
Event 1001: ShippingReady
Event 1002: Shipped
Event 1003: Delivered

상태 재구성:
1. 스냅샷부터 시작: { status: PAYMENT_COMPLETED }
2. 1001~1003 이벤트만 적용
3. 결과: { status: DELIVERED } ← 빠름!
```

---

## 6️⃣ CQRS 패턴

### 6.1 개념

**CQRS = Command Query Responsibility Segregation**

**전통적 방식** (단일 모델):
```
Request
├─ Command: 쓰기 (UPDATE, INSERT)
└─ Query: 읽기 (SELECT)

같은 데이터 모델 사용 → 최적화 어려움
```

**CQRS** (분리 모델):
```
Command Side (쓰기):
├─ 비즈니스 로직 중심
├─ 정규화 (중복 최소)
└─ 느려도 일관성 최우선

Event
┌──────────────────────┐
│  Kafka 메시지 큐     │
└──────────────────────┘
    ↓ (이벤트 구독)

Query Side (읽기):
├─ 조회 최적화
├─ 역정규화 (중복 허용)
└─ 빠른 응답
```

### 6.2 실무 예: 전자상거래

**Command Model**:
```
ORDER 테이블:
┌─────────┬────────────┬──────────┐
│ id      │ customer   │ status   │
├─────────┼────────────┼──────────┤
│ ord-001 │ cust-100   │ CREATED  │
└─────────┴────────────┴──────────┘

PAYMENT 테이블:
┌─────────┬────────┬──────────┐
│ order   │ method │ amount   │
├─────────┼────────┼──────────┤
│ ord-001 │ CARD   │ 100,000  │
└─────────┴────────┴──────────┘

정규화: 중복 없음, 저장 공간 절감
```

**Query Model**:
```
ORDER_VIEW 테이블:
┌─────────┬────────────┬──────────┬──────────┬─────────┐
│ id      │ customer   │ status   │ method   │ amount  │
├─────────┼────────────┼──────────┼──────────┼─────────┤
│ ord-001 │ cust-100   │ CREATED  │ CARD     │ 100,000 │
└─────────┴────────────┴──────────┴──────────┴─────────┘

역정규화: 모든 필요한 데이터 포함 → 조회 빠름!
```

### 6.3 최종 일관성 (Eventual Consistency)

**문제**:
```
1. 주문 생성 (Command) → 즉시 DB 저장
2. Event 발행 → Kafka 전송
3. Consumer 구독 → 지연 발생
4. Query Model 업데이트

따라서:
시간 T: 주문 생성 커맨드 실행
시간 T+100ms: Query에서 조회 → 아직 미반영 가능
시간 T+200ms: Query에서 조회 → 반영됨
```

**해결책**:
1. UI에서 즉시 응답 (낙관적 업데이트)
2. 백그라운드에서 동기화 추적
3. 문제 발생 시 알림

---

## 7️⃣ 면접 대비 Q&A

### Q1: Kafka의 순서 보장을 어떻게 하나요?
**A**:
```
같은 Key → 같은 Partition → 순서 보장

Key: order-001 → Hash → Partition 1
→ 항상 Partition 1에 저장
→ Consumer가 Partition 1에서 순서대로 읽음
→ 순서 보장! ✓
```

### Q2: Consumer Group이란?
**A**:
```
여러 Consumer가 같은 Topic을 구독하는 모임
각 Consumer는 다른 Partition 할당
→ 병렬 처리! 

예: 4개 Consumer, 4개 Partition
→ 각 Consumer가 1개씩 → 4배 빠름
```

### Q3: 메시지 손실을 방지하려면?
**A**:
```
Producer: acks=all (모든 복제본 받을 때까지 대기)
Consumer: 수동 커밋 (처리 후에만 커밋)
→ Exactly-once 보장!
```

### Q4: Event Sourcing의 장점?
**A**:
```
1. 완전한 감시 기록
2. 시간 여행 가능 (과거 상태 재현)
3. 버그 재현 용이
4. 성능 최적화: append-only (추가만 함)
```

### Q5: CQRS와 Event Sourcing의 차이?
**A**:
```
Event Sourcing: 이벤트 저장 방식 (HOW)
CQRS: 읽기/쓰기 분리 (WHAT)

함께 사용: Event Sourcing으로 이벤트 저장
         → CQRS로 읽기 모델 생성
```

---

## 🚀 실습 가이드

### Step 1: 프로젝트 시작
```bash
cd E:\dev\studdywrap\playground

# Kafka 인프라 시작
docker-compose up -d
```

### Step 2: API 테스트 (Postman 또는 cURL)

#### Producer 테스트
```bash
curl -X POST http://localhost:8080/api/kafka/producer/sync \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-001",
    "items": [
      {
        "productId": "prod-001",
        "productName": "노트북",
        "quantity": 1,
        "price": 1500000
      }
    ],
    "totalAmount": 1500000
  }'
```

#### Event Sourcing 테스트
```bash
# 주문 생성
curl -X POST http://localhost:8080/api/kafka/event-sourcing/create-order \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-001",
    "items": [],
    "totalAmount": 100000
  }'

# 주문 상태 조회
curl http://localhost:8080/api/kafka/event-sourcing/order/{orderId}

# 주문 히스토리 조회
curl http://localhost:8080/api/kafka/event-sourcing/order/{orderId}/history
```

#### CQRS 테스트
```bash
# 주문 생성 (Command)
curl -X POST http://localhost:8080/api/kafka/cqrs/create-order \
  -H "Content-Type: application/json" \
  -d '{...}'

# 주문 조회 (Query)
curl http://localhost:8080/api/kafka/cqrs/order/{orderId}

# 고객 주문 목록 (Query)
curl http://localhost:8080/api/kafka/cqrs/customer/{customerId}/orders

# 통계 (Query)
curl http://localhost:8080/api/kafka/cqrs/statistics
```

### Step 3: 로그 모니터링
```bash
# 실시간 로그 확인
docker-compose logs -f playground

# 특정 로그만 보기
docker-compose logs -f playground | grep "CQRS"
```

---

## 📝 중요 체크리스트 (면접 전)

- [ ] Kafka의 Topic, Partition, Broker, Consumer Group 개념 정리
- [ ] Producer: 동기 vs 비동기 차이
- [ ] Consumer: 자동 커밋 vs 수동 커밋
- [ ] 에러 처리: RetryableTopic, DLT
- [ ] Event Sourcing: 상태 재구성, 시간 여행
- [ ] CQRS: Command vs Query, 최종 일관성
- [ ] 순서 보장: Key 기반 파티셔닝
- [ ] 성능 최적화: 배치 처리, Snapshot

---

## 💡 실무 팁

1. **Producer 성능**: batch-size + linger-ms 조정
2. **Consumer 성능**: max-poll-records + concurrency 조정
3. **에러 처리**: 항상 DLT 모니터링
4. **모니터링**: Consumer Lag 추적 필수
5. **멱등성**: 항상 고려 (Kafka는 at-least-once)

---

**이 가이드로 내일 면접 성공하길! 🎉**
