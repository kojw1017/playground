## 🚀 Kafka 면접 준비 - 5분 요약 가이드

### 📍 위치
- **학습 자료**: `KAFKA_LEARNING_GUIDE.md` (상세 가이드)
- **Producer 코드**: `src/main/kotlin/com/example/playground/kafka/producer/KafkaProducerService.kt`
- **Consumer 코드**: `src/main/kotlin/com/example/playground/kafka/consumer/KafkaConsumerService.kt`
- **Event Sourcing**: `src/main/kotlin/com/example/playground/kafka/patterns/eventsourcing/EventSourcingService.kt`
- **CQRS 패턴**: `src/main/kotlin/com/example/playground/kafka/patterns/cqrs/CQRSService.kt`
- **API 컨트롤러**: `src/main/kotlin/com/example/playground/controller/KafkaController.kt`
- **설정 가이드**: `src/main/resources/application-kafka.yml`

---

## ⚡ 핵심 개념 30초 정리

### 1. Kafka의 5가지 핵심
```
Topic (주제)    → 메시지 카테고리
Partition (분할) → Topic의 분산 저장소 (병렬 처리!)
Broker (서버)   → Kafka 서버
Consumer Group  → 같은 Topic 구독하는 그룹
Offset (위치)   → 메시지 번호
```

### 2. Producer vs Consumer
```
Producer: 메시지를 Kafka에 보냄
├─ 동기: .get() 사용 → 느리지만 안전
└─ 비동기: .whenComplete() 사용 → 빠르지만 복잡

Consumer: Kafka에서 메시지를 받음
├─ 단일: 메시지 하나씩 처리
└─ 배치: 여러 개를 한 번에 처리 (20배 빠름!)
```

### 3. 순서 보장 (면접 필수!)
```
❌ 잘못된 이해: "Kafka는 순서를 보장하지 않는다"
✅ 올바른 이해: "같은 Key → 같은 Partition → 순서 보장!"

예: Key=order-001 → Hash → Partition 1
   → Partition 1의 모든 메시지는 순서 유지
```

### 4. 에러 처리
```
RetryableTopic:
  topic-events (실패)
    ↓ (1초 대기)
  topic-events-retry-0 (재실패)
    ↓ (2초 대기)
  topic-events-retry-1 (재실패)
    ↓ (4초 대기)
  topic-events-dlt (최종 실패 - 수동 개입)
```

### 5. Event Sourcing vs CQRS
```
Event Sourcing: 이벤트를 저장 (모든 변화 기록)
├─ 장점: 과거 상태 재현, 완전한 감시 기록
└─ 사용: 금융, 주문 이력 추적

CQRS: 읽기와 쓰기를 분리
├─ Command (쓰기): 비즈니스 로직
├─ Query (읽기): 조회 최적화
└─ 연결: Event로 동기화 (최종 일관성)
```

---

## 🎯 면접 질문별 답변

### Q1: "Kafka와 메시지 큐의 차이?"
**A**: 
```
메시지 큐 (RabbitMQ, ActiveMQ):
- 메시지를 전달하면 제거 (Fire-and-forget)
- 낮은 처리량

Kafka:
- 메시지를 디스크에 영속 저장
- Consumer Group이 독립적으로 구독 가능
- 높은 처리량 (초당 수백만 개)
- 재처리 가능
```

### Q2: "순서를 보장하면서 높은 처리량을 얻으려면?"
**A**:
```
1. 같은 순서가 필요한 메시지들은 같은 Key 사용
2. 다른 Key는 다른 Partition에 분산
3. 결과: Partition별로 병렬 처리 + 같은 Key는 순서 보장

예시:
Key=order-001 → Partition 1 (1개 Consumer)
Key=order-002 → Partition 2 (1개 Consumer)
Key=order-003 → Partition 3 (1개 Consumer)
→ 3개 Consumer가 병렬로 처리! 순서도 보장!
```

### Q3: "중복 처리는 어떻게 방지?"
**A**:
```
Kafka는 at-least-once 보장 (중복 가능)

해결책:
1. Producer: Idempotent 설정
   enable.idempotence=true
   → 같은 메시지 중복 제거

2. Consumer: 멱등성 처리
   - DB에 unique constraint 추가
   - 이미 처리된 eventId 스킵
   - DB 업데이트 + Offset 커밋을 원자적으로

결과: Exactly-once 보장!
```

### Q4: "Consumer가 느린데 처리량을 높이려면?"
**A**:
```
방법 1: Consumer 인스턴스 증가
- Consumer 1, 2, 3 추가
- 각각 다른 Partition 할당
- N개 Consumer = N배 처리량

방법 2: 배치 처리
- 메시지 1개당 처리가 아니라
- N개씩 모아서 처리
- DB 쿼리 수 감소 = 성능 향상

방법 3: 비동기 처리
- Consumer는 즉시 반환
- 별도 스레드풀에서 처리
- 다음 메시지 바로 수신
```

### Q5: "Event Sourcing을 왜 사용?"
**A**:
```
전통방식 (상태만 저장):
Order Table: { id: 001, status: SHIPPED }
→ 언제 SHIPPED가 됐는지? 왜 변경됐는지? 모름!

Event Sourcing:
Event 1: OrderCreated (10:00)
Event 2: PaymentReceived (10:05)
Event 3: ShippingReady (10:10)
Event 4: Shipped (10:15)
→ 모든 변화가 기록됨!
→ 10:10 시점의 상태를 다시 만들 수 있음! (시간 여행)
→ 버그 원인 파악 쉬움
```

### Q6: "CQRS의 최종 일관성 문제를 어떻게 해결?"
**A**:
```
문제: 주문 생성 후 Query에서 바로 조회하면 데이터 없을 수 있음

해결책:
1. UI: 낙관적 업데이트 (사용자에게 즉시 보여주기)
2. 백엔드: 동기화 모니터링
   - Query가 업데이트될 때까지 대기
   - 일반적으로 100ms 이내

3. 중요 데이터: 읽기 후 쓰기 일관성
   - User 생성 → 생성된 User 즉시 조회
   - 재시도 로직 구현
```

---

## 💻 실제 코드 맛보기

### Producer 동기 전송
```kotlin
fun sendOrderEventSync(event: OrderCreatedEvent): Boolean {
    return try {
        val result = kafkaTemplate.send("order-events", event.orderId, event)
            .get()  // ← 동기 대기
        log.info("전송 성공: partition={}, offset={}", 
            result.recordMetadata.partition(),
            result.recordMetadata.offset())
        true
    } catch (e: Exception) {
        log.error("전송 실패", e)
        false
    }
}
```

### Consumer 배치 처리
```kotlin
@KafkaListener(topics = ["order-events"], groupId = "order-group")
fun listenBatch(events: List<OrderCreatedEvent>) {
    log.info("배치 수신: {}개 메시지", events.size)
    
    // 배치 처리: 성능 20배 향상!
    events.forEach { event ->
        processOrder(event)
    }
    
    // 모두 성공 시 커밋
    ack.acknowledge()
}
```

### Event Sourcing
```kotlin
fun getOrderStateAtTime(orderId: String, targetTime: LocalDateTime): OrderState {
    val events = eventStore[orderId]
    
    // targetTime 이전의 이벤트만 적용
    return events
        .filter { it.timestamp.isBefore(targetTime) }
        .fold(OrderState()) { state, event ->
            when (event.eventType) {
                "OrderCreated" → state.copy(status = CREATED)
                "PaymentProcessed" → state.copy(status = PAYMENT_COMPLETED)
                "Shipped" → state.copy(status = SHIPPED)
                else → state
            }
        }
}
```

### CQRS
```kotlin
// Command: 주문 생성
@KafkaListener(topics = ["order-events"], groupId = "cqrs-group")
fun handleOrderCreated(event: OrderCreatedEvent) {
    // 1. Query Model 생성 (읽기용)
    val queryModel = OrderQueryModel(
        orderId = event.orderId,
        status = CREATED,
        totalAmount = event.totalAmount
    )
    queryStore[event.orderId] = queryModel
    
    // 2. Index 생성 (빠른 조회)
    customerOrderIndex[event.customerId].add(event.orderId)
}

// Query: 고객의 모든 주문 조회
fun getCustomerOrders(customerId: String): List<OrderQueryModel> {
    return customerOrderIndex[customerId]  // 빠른 조회!
        .mapNotNull { queryStore[it] }
}
```

---

## 📋 면접 전 필독 체크리스트

### 개념 이해 (필수!)
- [ ] Topic과 Partition의 관계
- [ ] Consumer Group의 역할
- [ ] Offset과 Commit의 차이
- [ ] 동기 vs 비동기 전송
- [ ] 배치 처리의 이점
- [ ] 순서 보장 메커니즘
- [ ] 중복 처리 방지 방법
- [ ] Event Sourcing의 시간 여행
- [ ] CQRS의 Command와 Query
- [ ] 최종 일관성

### 실무 능력 (가능하면!)
- [ ] Producer 코드 읽고 이해
- [ ] Consumer 코드 읽고 이해
- [ ] RetryableTopic 설정 이해
- [ ] 에러 처리 패턴
- [ ] Event Sourcing 구현 원리
- [ ] CQRS 구현 원리

### 답변 연습 (강력 추천!)
- [ ] "Kafka와 메시지 큐의 차이?"
- [ ] "순서를 보장하면서 높은 처리량?"
- [ ] "중복 처리 방지법?"
- [ ] "Consumer Lag란?"
- [ ] "Event Sourcing의 장단점?"
- [ ] "CQRS의 최종 일관성?"

---

## 🎬 빠른 시작 (10분)

### 1단계: Kafka 인프라 시작 (1분)
```bash
cd E:\dev\studdywrap\playground
docker-compose up -d
```

### 2단계: 애플리케이션 빌드 (3분)
```bash
./gradlew clean build -x test
```

### 3단계: 애플리케이션 실행 (2분)
```bash
./gradlew bootRun
```

### 4단계: API 테스트 (4분)
```bash
# 정보 조회
curl http://localhost:8080/api/kafka/info

# Producer 동기 전송
curl -X POST http://localhost:8080/api/kafka/producer/sync \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-001","items":[],"totalAmount":100000}'

# Event Sourcing 주문 생성
curl -X POST http://localhost:8080/api/kafka/event-sourcing/create-order \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-001","items":[],"totalAmount":100000}'

# CQRS 통합 워크플로우
curl -X POST http://localhost:8080/api/kafka/workflow/execute
```

---

## 📚 학습 순서 (내일 아침)

### 6:00 ~ 7:00 (1시간) - 기초 개념
1. Topic, Partition, Broker 이해
2. Consumer Group 동작 원리
3. Offset과 Commit
4. 순서 보장 메커니즘

### 7:00 ~ 8:00 (1시간) - Producer & Consumer
1. 동기 vs 비동기 전송
2. 배치 처리 이점
3. 에러 처리 및 재시도
4. 중복 처리 방지

### 8:00 ~ 9:00 (1시간) - 고급 패턴
1. Event Sourcing
2. CQRS
3. 실제 사용 사례

### 9:00 ~ 면접 (복습)
1. 일반적인 면접 질문 답변 연습
2. 코드 예제 복습
3. 마음가짐!

---

## 🎯 마지막 조언

### 면접 시 좋은 답변 팁:
1. **구체적인 예시를 들기**
   - ❌ "Kafka는 높은 처리량을 제공합니다"
   - ✅ "Kafka는 100개 Partition에서 초당 100만 메시지를 병렬 처리할 수 있습니다"

2. **트레이드오프를 이해하기**
   - ❌ "동기 전송이 좋습니다"
   - ✅ "동기 전송은 안정적이지만 느리고, 비동기는 빠르지만 복잡합니다. 상황에 따라 선택합니다"

3. **실무 경험 연결하기**
   - ❌ "Event Sourcing을 알고 있습니다"
   - ✅ "Event Sourcing으로 모든 변화를 기록하면, 과거 특정 시점의 상태를 재현할 수 있어서 버그 원인 파악이 쉽습니다"

### 면접 중 모르는 것이 나왔을 때:
1. **솔직하게 인정하기**
   - "이 부분은 아직 경험이 없지만, 이렇게 학습할 것 같습니다"

2. **유사 경험 연결하기**
   - "Kafka는 모르지만, RabbitMQ를 사용해봤고, 차이점은..."

3. **질문으로 돌려받기**
   - "Kafka의 X 부분이 어떻게 구현되어 있는지 궁금합니다"

---

**화이팅! 내일 면접 꼭 성공하세요! 🚀**

마지막 팁: 코드를 직접 읽고, 로그를 보고, API를 테스트해보세요. 그것이 가장 좋은 학습 방법입니다!
