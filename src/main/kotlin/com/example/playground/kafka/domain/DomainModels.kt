package com.example.playground.kafka.domain

import java.time.LocalDateTime

/**
 * Kafka 학습용 도메인 모델
 *
 * 실무 시나리오: 전자상거래 플랫폼
 * - 주문 생성 → 결제 처리 → 배송 준비 → 배송 완료
 * 각 단계가 Event로 발행되고, 각 마이크로서비스가 구독
 */

// ============================================================================
// 1. Event 기본 구조 (Event Sourcing)
// ============================================================================

/**
 * 모든 이벤트의 기본 인터페이스
 * - 불변(Immutable)
 * - 타임스탬프 포함
 * - 추적 가능한 ID 포함
 */
data class DomainEvent(
    val eventId: String,              // 고유 이벤트 ID
    val aggregateId: String,          // 어그리게이트(주문) ID
    val eventType: String,            // 이벤트 타입
    val timestamp: LocalDateTime = LocalDateTime.now(),
    val payload: Map<String, Any>     // 실제 데이터
)

// ============================================================================
// 2. 주문 관련 이벤트들
// ============================================================================

/**
 * 주문 생성 이벤트
 * Topic: order-events
 * Partition Key: orderId (같은 주문은 같은 파티션)
 */
data class OrderCreatedEvent(
    val orderId: String,
    val customerId: String,
    val totalAmount: Double,
    val items: List<OrderItem>,
    val createdAt: LocalDateTime = LocalDateTime.now(),
    val eventId: String = java.util.UUID.randomUUID().toString()
)

/**
 * 결제 완료 이벤트
 * Topic: payment-events
 */
data class PaymentCompletedEvent(
    val paymentId: String,
    val orderId: String,
    val amount: Double,
    val paymentMethod: String,      // CREDIT_CARD, DEBIT_CARD, etc.
    val completedAt: LocalDateTime = LocalDateTime.now(),
    val eventId: String = java.util.UUID.randomUUID().toString()
)

/**
 * 배송 준비 완료 이벤트
 * Topic: shipping-events
 */
data class ShippingPreparedEvent(
    val shippingId: String,
    val orderId: String,
    val warehouseId: String,
    val estimatedDeliveryDate: LocalDateTime,
    val preparedAt: LocalDateTime = LocalDateTime.now(),
    val eventId: String = java.util.UUID.randomUUID().toString()
)

/**
 * 배송 완료 이벤트
 * Topic: shipping-events
 */
data class ShippingCompletedEvent(
    val shippingId: String,
    val orderId: String,
    val trackingNumber: String,
    val completedAt: LocalDateTime = LocalDateTime.now(),
    val eventId: String = java.util.UUID.randomUUID().toString()
)

// ============================================================================
// 3. 보조 도메인 모델
// ============================================================================

data class OrderItem(
    val productId: String,
    val productName: String,
    val quantity: Int,
    val price: Double
)

// ============================================================================
// 4. Command 모델 (CQRS)
// ============================================================================

/**
 * 커맨드: 변경을 요청하는 객체
 * - 단일 책임: 한 가지 작업만 수행
 * - 동기적이거나 비동기적 처리
 */
data class CreateOrderCommand(
    val orderId: String,
    val customerId: String,
    val items: List<OrderItem>,
    val totalAmount: Double
)

data class ProcessPaymentCommand(
    val orderId: String,
    val paymentId: String,
    val amount: Double,
    val paymentMethod: String
)

data class PrepareShippingCommand(
    val orderId: String,
    val shippingId: String,
    val warehouseId: String
)

// ============================================================================
// 5. Query 모델 (CQRS)
// ============================================================================

/**
 * 쿼리: 읽기 요청
 * - 최신 상태만 필요
 * - 실시간 업데이트 (Event로부터 파생)
 * - 캐시 가능
 */
data class OrderQueryModel(
    val orderId: String,
    val customerId: String,
    val status: OrderStatus,
    val totalAmount: Double,
    val createdAt: LocalDateTime,
    val lastUpdatedAt: LocalDateTime
)

enum class OrderStatus {
    CREATED,
    PAYMENT_PENDING,
    PAYMENT_COMPLETED,
    SHIPPING_PREPARED,
    SHIPPING_IN_TRANSIT,
    DELIVERED,
    CANCELLED
}

// ============================================================================
// 6. 메시지 래퍼 (Kafka 메시지 헤더)
// ============================================================================

/**
 * Kafka 메시지에 부가 정보(메타데이터) 포함
 * - 타임스탐프
 * - 소스 시스템
 * - 상관관계 ID
 */
data class EventMessage<T>(
    val eventId: String = java.util.UUID.randomUUID().toString(),
    val eventType: String,
    val timestamp: LocalDateTime = LocalDateTime.now(),
    val source: String,              // 발행한 시스템 (order-service, payment-service)
    val correlationId: String,       // 요청 추적 ID
    val payload: T
)

// ============================================================================
// 7. Kafka 메시지 헤더 정의
// ============================================================================

object KafkaHeaders {
    const val EVENT_ID = "eventId"
    const val EVENT_TYPE = "eventType"
    const val CORRELATION_ID = "correlationId"
    const val SOURCE = "source"
    const val TIMESTAMP = "timestamp"
}
