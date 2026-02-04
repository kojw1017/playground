# Kafka 속성 교육 프로젝트 구조

## 📦 전체 프로젝트 트리

```
playground/
├── 📄 KAFKA_LEARNING_GUIDE.md          ← 상세 학습 가이드 (필독!)
├── 📄 KAFKA_QUICK_REFERENCE.md         ← 5분 요약 + 면접 팁
├── 📄 README.md                        ← 이 파일
│
├── src/main/kotlin/com/example/playground/
│   ├── 📁 kafka/
│   │   ├── 📁 domain/
│   │   │   └── DomainModels.kt         ← 주문, 결제, 배송 이벤트 정의
│   │   │
│   │   ├── 📁 producer/
│   │   │   └── KafkaProducerService.kt ← Producer 구현 (동기/비동기/배치/헤더)
│   │   │
│   │   ├── 📁 consumer/
│   │   │   └── KafkaConsumerService.kt ← Consumer 구현 (단일/배치/재시도/수동커밋)
│   │   │
│   │   └── 📁 patterns/
│   │       ├── 📁 eventsourcing/
│   │       │   └── EventSourcingService.kt ← Event Sourcing 패턴 (시간 여행!)
│   │       │
│   │       └── 📁 cqrs/
│   │           └── CQRSService.kt      ← CQRS 패턴 (읽기/쓰기 분리)
│   │
│   ├── 📁 controller/
│   │   └── KafkaController.kt          ← REST API 엔드포인트 (테스트용)
│   │
│   └── ... 기타 서비스들 ...
│
├── src/main/resources/
│   ├── application.yml                 ← 기본 설정
│   ├── application-kafka.yml           ← Kafka 전용 설정 (상세 주석)
│   └── ... 기타 리소스 ...
│
├── build.gradle                        ← Gradle 빌드 설정 (Spring Boot 3.5, Kotlin 1.9)
├── docker-compose.yml                  ← Kafka + Zookeeper + Kafka-UI
└── gradlew / gradlew.bat              ← Gradle 래퍼
```

---

## 🎯 핵심 파일 설명

### 1. DomainModels.kt (도메인 모델)
**목적**: Kafka 메시지의 데이터 구조 정의

**포함 내용**:
```kotlin
- OrderCreatedEvent         // 주문 생성 이벤트
- PaymentCompletedEvent     // 결제 완료 이벤트
- ShippingPreparedEvent     // 배송 준비 이벤트
- ShippingCompletedEvent    // 배송 완료 이벤트
- OrderQueryModel           // CQRS 읽기 모델
- CreateOrderCommand        // 주문 생성 커맨드
- ProcessPaymentCommand     // 결제 처리 커맨드
```

**Key 개념**:
- 이벤트는 불변(Immutable)
- 타임스탬프 포함
- 고유 ID 포함 (추적 가능)

---

### 2. KafkaProducerService.kt (메시지 발행)
**목적**: 다양한 Producer 패턴 구현

**메서드별 학습 포인트**:

| 메서드 | 특징 | 사용 시기 |
|--------|------|---------|
| `sendOrderEventSync()` | 동기 전송 | 결제, 금융 |
| `sendPaymentEventAsync()` | 비동기 전송 | 로깅, 분석 |
| `sendShippingEventWithPartitioning()` | 파티션 지정 | 순서 보장 필요 |
| `sendEventWithHeaders()` | 메타데이터 포함 | 추적 가능성 |
| `sendEventsInTransaction()` | 원자성 보장 | Exactly-once 필요 |
| `sendBatchOrders()` | 배치 처리 | 높은 처리량 |
| `sendProductionEvent()` | 프로덕션 레벨 | 실무 사용 |

**학습 순서**:
1. sendOrderEventSync() 이해 → 동기/비동기 개념
2. sendPaymentEventAsync() 이해 → 콜백 처리
3. sendBatchOrders() 이해 → 성능 최적화
4. sendProductionEvent() 이해 → 실무 적용

---

### 3. KafkaConsumerService.kt (메시지 구독)
**목적**: 다양한 Consumer 패턴 구현

**메서드별 학습 포인트**:

| 메서드 | 특징 | 포인트 |
|--------|------|-------|
| `listenOrderEvents()` | 기본 Consumer | Consumer Group, Partition |
| `listenPaymentEventsBatch()` | 배치 Consumer | 성능 20배 향상 |
| `listenShippingEventsWithRetry()` | 재시도 | RetryableTopic, Backoff |
| `handleShippingDLT()` | Dead Letter Topic | 최종 실패 처리 |
| `listenOrderEventsWithHeaders()` | 메타데이터 처리 | 헤더 추출, 추적 |
| `listenCriticalEventsWithManualCommit()` | 수동 커밋 | Exactly-once 보장 |

**에러 처리 흐름**:
```
shipping-events (실패)
    ↓ Backoff: 1초
shipping-events-retry-0 (실패)
    ↓ Backoff: 2초
shipping-events-retry-1 (실패)
    ↓ Backoff: 4초
shipping-events-dlt (최종 실패 → 수동 개입)
```

---

### 4. EventSourcingService.kt (이벤트 저장 패턴)
**목적**: 모든 변화를 이벤트로 기록

**핵심 메서드**:

```kotlin
// 1. 이벤트 발행
createOrder(cmd)                    // 주문 생성 → Event 저장

// 2. 상태 재구성
reconstructOrderState(orderId)      // 현재 상태 = 모든 이벤트 누적

// 3. 시간 여행
getOrderStateAtTime(orderId, time)  // 과거 특정 시점의 상태 조회

// 4. 히스토리
getOrderHistory(orderId)            // 감시 로그 조회

// 5. 성능 최적화
createSnapshot(orderId)             // 상태 캐싱
```

**Time Travel 예시**:
```
2026-01-26 10:00 - 주문 생성
2026-01-26 10:05 - 결제 완료
2026-01-26 10:10 - 배송 준비
2026-01-26 10:15 - 배송 완료

요청: "10:10 시점의 상태는?"
응답: { status: SHIPPING_PREPARED }
```

---

### 5. CQRSService.kt (읽기/쓰기 분리 패턴)
**목적**: 읽기와 쓰기를 완전히 분리하여 각각 최적화

**구조**:

```
Command Model (쓰기)          Event          Query Model (읽기)
─────────────────────────────────────────────────────────────
• Order Table                Kafka           • OrderView Table
• Payment Table             (토픽)           • CustomerIndexView
• 정규화                                     • 역정규화
• 느려도 일관성 우선                         • 빠른 조회
```

**메서드 분류**:

| 타입 | 메서드 | 역할 |
|-----|--------|------|
| Command | createOrderCommand() | 주문 생성 |
| Command | processPaymentCommand() | 결제 처리 |
| Query | getOrderDetails() | 주문 상세 조회 |
| Query | getCustomerOrders() | 고객 주문 목록 |
| Query | getOrderStatistics() | 주문 통계 |
| Handler | handleOrderCreatedEvent() | Event → Query Model 동기화 |

**최종 일관성 문제**:
```
시간 T+0ms:   주문 생성 커맨드 실행
시간 T+50ms:  Query 조회 → 아직 데이터 없음! 😱
시간 T+150ms: Query 조회 → 이제 보임! 😊
```

---

### 6. KafkaController.kt (REST API)
**목적**: 학습한 모든 개념을 API로 테스트

**엔드포인트 카테고리**:

#### Producer API
```bash
POST /api/kafka/producer/sync           # 동기 전송
POST /api/kafka/producer/async          # 비동기 전송
POST /api/kafka/producer/batch          # 배치 전송
POST /api/kafka/producer/with-headers   # 헤더 포함
```

#### Event Sourcing API
```bash
POST /api/kafka/event-sourcing/create-order                    # 주문 생성
GET  /api/kafka/event-sourcing/order/{orderId}                # 현재 상태
GET  /api/kafka/event-sourcing/order/{orderId}/history        # 히스토리
GET  /api/kafka/event-sourcing/order/{orderId}/at-time        # 시간 여행
GET  /api/kafka/event-sourcing/statistics                     # 통계
```

#### CQRS API
```bash
POST /api/kafka/cqrs/create-order                             # 주문 생성 (Command)
GET  /api/kafka/cqrs/order/{orderId}                          # 주문 조회 (Query)
GET  /api/kafka/cqrs/customer/{customerId}/orders             # 고객 주문 (Query)
GET  /api/kafka/cqrs/statistics                               # 통계 (Query)
```

#### 통합 API
```bash
POST /api/kafka/workflow/execute                              # 전체 워크플로우
GET  /api/kafka/info                                          # API 정보
```

---

### 7. application-kafka.yml (상세 설정)
**목적**: Kafka 성능 튜닝 및 설정 가이드

**주요 설정**:

```yaml
# Producer 성능 최적화
producer:
  batch-size: 16384        # 16KB 배치
  linger-ms: 10            # 10ms 대기 (배치 모음)
  compression-type: snappy # 압축

# Consumer 신뢰성
consumer:
  auto-offset-reset: earliest      # 없는 오프셋은 처음부터
  enable-auto-commit: false        # 수동 커밋 (정확성)
  max-poll-records: 500            # 한 번에 500개

# 트랜잭션
producer:
  transaction-id-prefix: playground-tx-
  properties:
    enable.idempotence: true       # 중복 제거
```

---

## 🚀 실습 시나리오

### 시나리오 1: Producer 동작 원리 이해
```
1. KafkaProducerService.sendOrderEventSync() 호출
2. 메시지가 Broker에 저장될 때까지 대기 (.get())
3. RecordMetadata 반환 (partition, offset)
4. 로그: "[SYNC] 주문 생성 이벤트 전송 성공 - partition=1, offset=1234"
```

### 시나리오 2: Consumer Lag 이해
```
1. Consumer Group: order-service-group
2. Topic: order-events (3개 Partition)
3. 각 Consumer가 1개씩 할당
4. Consumer Lag = LogEndOffset - CommittedOffset
   예: 1000 - 850 = 150 (150개 미처리)
```

### 시나리오 3: Event Sourcing으로 버그 재현
```
1. 고객: "어제 10:30의 주문 상태는?"
2. API: GET /event-sourcing/order/ord-001/at-time?time=2026-01-25T10:30:00
3. 시스템: 10:30 이전의 이벤트만 재현
4. 응답: { status: SHIPPING_PREPARED } ← 완전히 재현됨!
```

### 시나리오 4: CQRS로 높은 처리량 달성
```
1. Command: createOrderCommand() → OrderCommandModel 저장
2. Event: order-events 발행
3. Consumer: handleOrderCreatedEvent() 구독
4. Query: OrderQueryModel 생성
5. 사용자: getCustomerOrders() 호출 → 매우 빠른 응답!
```

---

## 📚 학습 전개

### Day 1 아침 (면접 3시간 전)
1. **30분**: KAFKA_LEARNING_GUIDE.md 읽기
   - 1~3장: 기초 개념
   
2. **30분**: KafkaProducerService.kt 코드 읽기
   - sendOrderEventSync()부터 시작
   - 각 메서드의 주석 읽기
   
3. **30분**: KafkaConsumerService.kt 코드 읽기
   - listenOrderEvents()부터 시작
   - RetryableTopic 이해
   
4. **30분**: KAFKA_QUICK_REFERENCE.md 읽기
   - 면접 질문 답변 연습
   - 자주 틀리는 부분 재학습

### Day 1 아침 (면접 1시간 전)
1. **20분**: EventSourcingService.kt 읽기
2. **20분**: CQRSService.kt 읽기
3. **20분**: 면접 예상 질문 5개 답변 연습

---

## 💡 코드 읽는 팁

### 1. 주석 읽기
```kotlin
// 각 메서드의 윗부분에 있는 주석을 먼저 읽기
// ========================================================================
// 1. 기초: 동기 전송 (Synchronous Send)
// ========================================================================
/**
 * 동기 전송: ...
 * 
 * 장점: ...
 * 단점: ...
 * 사용 시기: ...
 */
```

### 2. 로그 메시지 읽기
```kotlin
// 로그 메시지가 실행 흐름을 설명함
log.info(
    "[SYNC] 주문 생성 이벤트 전송 성공 - " +
    "orderId={}, partition={}, offset={}",
    event.orderId,
    result.recordMetadata.partition(),
    result.recordMetadata.offset()
)
```

### 3. 메서드 이름으로 흐름 파악
```kotlin
sendOrderEventSync()        // 무엇을 하는가? 동기 발송
sendPaymentEventAsync()     // 무엇을 하는가? 비동기 발송
sendEventWithHeaders()      // 무엇을 하는가? 헤더 포함 발송
```

---

## ✅ 완료 체크리스트

프로젝트를 다 이해했다면:
- [ ] Topic, Partition, Broker의 관계를 그릴 수 있다
- [ ] Producer 동기/비동기의 트레이드오프를 설명할 수 있다
- [ ] Consumer Group이 어떻게 로드 밸런싱하는지 설명할 수 있다
- [ ] Event Sourcing으로 시간 여행이 가능한 이유를 설명할 수 있다
- [ ] CQRS에서 최종 일관성 문제를 설명할 수 있다
- [ ] 각 코드를 직접 읽고 이해할 수 있다

---

## 🎯 면접 팁

### Good Answer
```
Q: "Kafka의 순서를 어떻게 보장하나요?"

A: "같은 Key를 가진 메시지는 같은 Partition으로 라우팅되고, 
    Partition 내에서는 순서가 보장됩니다. 
    
    예를 들어, orderId를 Key로 사용하면 같은 주문의 모든 이벤트
    (OrderCreated → PaymentProcessed → Shipped)는 같은 
    Partition에서 순서대로 처리됩니다."
```

### Bad Answer
```
Q: "Kafka의 순서를 어떻게 보장하나요?"

A: "Kafka는 순서를 보장합니다."

❌ 너무 간단함
❌ 구체적이지 않음
❌ 실제 메커니즘을 모르는 것 같음
```

---

**화이팅! 이 프로젝트로 Kafka를 완벽히 마스터하세요! 🚀**
