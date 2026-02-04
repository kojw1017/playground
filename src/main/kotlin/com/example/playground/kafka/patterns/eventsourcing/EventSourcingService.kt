package com.example.playground.kafka.patterns.eventsourcing

import com.example.playground.kafka.domain.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap

/**
 * Event Sourcing 패턴
 *
 * 핵심 개념:
 * 1. 상태(State) 저장 대신 이벤트(Event) 저장
 * 2. 현재 상태 = 모든 이벤트의 누적
 * 3. 시간 여행 가능 (과거의 특정 시점 상태 재현)
 * 4. 감사 추적(Audit Trail) 자동 제공
 *
 * 장점:
 * - 완전한 감시 기록: 모든 변경이 기록됨
 * - 버그 재현 가능: 과거 이벤트 재실행
 * - 시간 복원력: 언제든 과거 상태 조회
 * - 성능 최적화: 이벤트는 추가만 함 (Append-only)
 *
 * 단점:
 * - 복잡함: 개념 이해 어려움
 * - 메모리: 모든 이벤트 저장
 * - 조회 성능: 현재 상태 구성에 시간 소요
 *
 * 실무 사용 예:
 * - 금융 거래: 모든 거래 기록 필수
 * - 주문 관리: 주문 변경 이력 추적
 * - 사용자 권한: 권한 변경 감시
 */
@Service
class EventSourcingService {
    private val log = LoggerFactory.getLogger(EventSourcingService::class.java)

    // Event Store: 모든 이벤트를 시간 순서대로 저장
    // 실무: RocksDB, PostgreSQL JSONB 등 사용
    private val eventStore = ConcurrentHashMap<String, MutableList<DomainEvent>>()

    // Snapshot Store: 성능 최적화를 위해 특정 시점의 상태 저장
    // 예: 1000개 이벤트마다 스냅샷 생성
    private val snapshotStore = ConcurrentHashMap<String, OrderSnapshot>()

    // ========================================================================
    // 1. 이벤트 발행 (Event Publishing)
    // ========================================================================
    /**
     * 주문 생성 커맨드 → 주문 생성 이벤트 발행
     *
     * Command Pattern:
     * Command (CreateOrder) → Domain Logic → Events (OrderCreated)
     *
     * Event Sourcing에서:
     * - 커맨드를 실행하기 전에 검증
     * - 성공하면 이벤트를 저장하고 발행
     * - 실패하면 이벤트를 저장하지 않음
     */
    fun createOrder(command: CreateOrderCommand): String {
        log.info("[ES] 주문 생성 커맨드 처리 시작 - orderId={}", command.orderId)

        // 1. 커맨드 검증
        require(command.items.isNotEmpty()) { "주문에는 최소 1개 이상의 상품이 필요합니다" }
        require(command.totalAmount > 0) { "주문 금액은 0보다 커야 합니다" }

        // 2. 이벤트 생성
        val event = DomainEvent(
            eventId = java.util.UUID.randomUUID().toString(),
            aggregateId = command.orderId,
            eventType = "OrderCreated",
            timestamp = LocalDateTime.now(),
            payload = mapOf(
                "customerId" to command.customerId,
                "items" to command.items,
                "totalAmount" to command.totalAmount
            )
        )

        // 3. 이벤트 저장 (Event Store)
        eventStore.computeIfAbsent(command.orderId) { mutableListOf() }.add(event)

        log.info(
            "[ES] 주문 생성 이벤트 저장됨 - orderId={}, eventId={}",
            command.orderId,
            event.eventId
        )

        // 4. 이벤트 발행 (Kafka)
        // 실무: kafkaProducerService.send("order-events", event)

        return command.orderId
    }

    /**
     * 주문에 대한 결제 처리 커맨드
     */
    fun processPayment(command: ProcessPaymentCommand): String {
        log.info("[ES] 결제 처리 커맨드 - orderId={}, paymentId={}", command.orderId, command.paymentId)

        // 1. 현재 상태 조회 (이벤트들로부터 재구성)
        val currentState = reconstructOrderState(command.orderId)

        // 2. 상태 검증
        require(currentState != null) { "주문을 찾을 수 없습니다: ${command.orderId}" }
        require(currentState.status == OrderStatus.CREATED) { "주문이 CREATED 상태가 아닙니다" }

        // 3. 이벤트 생성
        val event = DomainEvent(
            eventId = java.util.UUID.randomUUID().toString(),
            aggregateId = command.orderId,
            eventType = "PaymentProcessed",
            timestamp = LocalDateTime.now(),
            payload = mapOf(
                "paymentId" to command.paymentId,
                "amount" to command.amount,
                "paymentMethod" to command.paymentMethod
            )
        )

        // 4. 이벤트 저장
        eventStore.computeIfAbsent(command.orderId) { mutableListOf() }.add(event)

        log.info(
            "[ES] 결제 처리 이벤트 저장됨 - orderId={}, paymentId={}",
            command.orderId,
            command.paymentId
        )

        return command.paymentId
    }

    // ========================================================================
    // 2. 현재 상태 재구성 (State Reconstruction)
    // ========================================================================
    /**
     * Event Sourcing의 핵심:
     * 현재 상태 = 모든 이벤트를 처음부터 끝까지 순서대로 적용
     *
     * 구현 전략:
     * 1. 스냅샷 확인: 최신 스냅샷 찾기
     * 2. 스냅샷 이후의 이벤트만 적용
     * 3. 최종 상태 반환
     *
     * 성능 최적화:
     * - 1000개 이벤트마다 스냅샷 생성
     * - 최신 스냅샷 + 그 이후 이벤트만 재현
     */
    fun reconstructOrderState(orderId: String): OrderQueryModel? {
        val events = eventStore[orderId] ?: return null

        log.debug("[ES] 주문 상태 재구성 시작 - orderId={}, 총 이벤트: {}", orderId, events.size)

        // 1. 스냅샷 확인
        var state = snapshotStore[orderId]?.let {
            OrderQueryModel(
                orderId = it.orderId,
                customerId = it.customerId,
                status = it.status,
                totalAmount = it.totalAmount,
                createdAt = it.createdAt,
                lastUpdatedAt = it.lastUpdatedAt
            )
        }

        // 2. 이벤트 적용
        events.forEach { event ->
            when (event.eventType) {
                "OrderCreated" -> {
                    state = OrderQueryModel(
                        orderId = orderId,
                        customerId = event.payload["customerId"] as String,
                        status = OrderStatus.CREATED,
                        totalAmount = event.payload["totalAmount"] as Double,
                        createdAt = event.timestamp,
                        lastUpdatedAt = event.timestamp
                    )
                }
                "PaymentProcessed" -> {
                    state = state?.copy(
                        status = OrderStatus.PAYMENT_COMPLETED,
                        lastUpdatedAt = event.timestamp
                    )
                }
                "ShippingPrepared" -> {
                    state = state?.copy(
                        status = OrderStatus.SHIPPING_PREPARED,
                        lastUpdatedAt = event.timestamp
                    )
                }
                "ShippingCompleted" -> {
                    state = state?.copy(
                        status = OrderStatus.DELIVERED,
                        lastUpdatedAt = event.timestamp
                    )
                }
            }
        }

        log.debug(
            "[ES] 주문 상태 재구성 완료 - orderId={}, status={}",
            orderId,
            state?.status
        )

        return state
    }

    // ========================================================================
    // 3. 이벤트 히스토리 조회 (Event History)
    // ========================================================================
    /**
     * 주문의 전체 이벤트 히스토리 조회
     *
     * 용도:
     * - 감시 로그 (Audit Log)
     * - 문제 디버깅
     * - 규정 준수 (Compliance)
     */
    fun getOrderHistory(orderId: String): List<DomainEvent> {
        val events = eventStore[orderId] ?: emptyList()

        log.info(
            "[ES] 주문 히스토리 조회 - orderId={}, 이벤트: {}개",
            orderId,
            events.size
        )

        return events.toList()
    }

    /**
     * 주문의 상태 변화 타임라인
     */
    fun getOrderTimeline(orderId: String): List<TimelineEntry> {
        val events = eventStore[orderId] ?: return emptyList()

        return events.mapIndexed { index, event ->
            TimelineEntry(
                sequence = index + 1,
                eventId = event.eventId,
                eventType = event.eventType,
                timestamp = event.timestamp,
                details = event.payload
            )
        }
    }

    // ========================================================================
    // 4. 시간 여행 (Time Travel) - 과거 특정 시점의 상태 조회
    // ========================================================================
    /**
     * Event Sourcing의 강점:
     * 특정 시점에서의 상태를 그대로 재현 가능
     *
     * 예:
     * - 2시간 전의 주문 상태는?
     * - 결제 전의 주문 상태는?
     *
     * 사용 시나리오:
     * - 문제 원인 파악: "언제 상태가 변했는가"
     * - 규정 준수: "특정 시점의 데이터 증명"
     * - 감시 기록: "모든 변경의 증거"
     */
    fun getOrderStateAtTime(orderId: String, targetTime: LocalDateTime): OrderQueryModel? {
        val events = eventStore[orderId] ?: return null

        // targetTime 이전의 이벤트만 적용
        val eventsBeforeTarget = events.filter { it.timestamp.isBefore(targetTime) }

        log.info(
            "[ES] 시간 여행 조회 - orderId={}, targetTime={}, 이벤트: {}개",
            orderId,
            targetTime,
            eventsBeforeTarget.size
        )

        var state: OrderQueryModel? = null

        eventsBeforeTarget.forEach { event ->
            when (event.eventType) {
                "OrderCreated" -> {
                    state = OrderQueryModel(
                        orderId = orderId,
                        customerId = event.payload["customerId"] as String,
                        status = OrderStatus.CREATED,
                        totalAmount = event.payload["totalAmount"] as Double,
                        createdAt = event.timestamp,
                        lastUpdatedAt = event.timestamp
                    )
                }
                "PaymentProcessed" -> {
                    state = state?.copy(
                        status = OrderStatus.PAYMENT_COMPLETED,
                        lastUpdatedAt = event.timestamp
                    )
                }
            }
        }

        return state
    }

    // ========================================================================
    // 5. 스냅샷 관리 (Snapshot Management)
    // ========================================================================
    /**
     * 성능 최적화: 스냅샷 생성
     *
     * 이유:
     * - 이벤트가 많으면 재구성 시간 증가
     * - 예: 1,000,000개 이벤트 = 1초 이상 소요
     * - 스냅샷: 특정 시점의 완성된 상태 저장
     *
     * 전략:
     * - 매 N개 이벤트마다 스냅샷 생성
     * - 또는 매 시간마다 생성
     */
    fun createSnapshot(orderId: String) {
        val state = reconstructOrderState(orderId) ?: return

        snapshotStore[orderId] = OrderSnapshot(
            orderId = state.orderId,
            customerId = state.customerId,
            status = state.status,
            totalAmount = state.totalAmount,
            createdAt = state.createdAt,
            lastUpdatedAt = LocalDateTime.now(),
            eventCount = eventStore[orderId]?.size ?: 0
        )

        log.info(
            "[ES] 스냅샷 생성됨 - orderId={}, eventCount={}",
            orderId,
            eventStore[orderId]?.size
        )
    }

    // ========================================================================
    // 6. 이벤트 통계
    // ========================================================================
    /**
     * Event Sourcing으로부터 통계 생성
     * 예: "오늘 몇 개의 주문이 생성되었는가?"
     */
    fun getOrderStatistics(): OrderStatistics {
        val allEvents = eventStore.values.flatten()

        val createdCount = allEvents.count { it.eventType == "OrderCreated" }
        val paymentCount = allEvents.count { it.eventType == "PaymentProcessed" }
        val shippingCount = allEvents.count { it.eventType == "ShippingCompleted" }

        return OrderStatistics(
            totalEvents = allEvents.size,
            ordersCreated = createdCount,
            paymentsProcessed = paymentCount,
            ordersShipped = shippingCount
        )
    }

    // ========================================================================
    // 보조 데이터 클래스
    // ========================================================================

    data class TimelineEntry(
        val sequence: Int,
        val eventId: String,
        val eventType: String,
        val timestamp: LocalDateTime,
        val details: Map<String, Any>
    )

    data class OrderSnapshot(
        val orderId: String,
        val customerId: String,
        val status: OrderStatus,
        val totalAmount: Double,
        val createdAt: LocalDateTime,
        val lastUpdatedAt: LocalDateTime,
        val eventCount: Int
    )

    data class OrderStatistics(
        val totalEvents: Int,
        val ordersCreated: Int,
        val paymentsProcessed: Int,
        val ordersShipped: Int
    )
}
