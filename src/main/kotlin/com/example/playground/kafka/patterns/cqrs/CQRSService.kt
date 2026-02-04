package com.example.playground.kafka.patterns.cqrs

import com.example.playground.kafka.domain.*
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentHashMap

/**
 * CQRS (Command Query Responsibility Segregation) 패턴
 *
 * 핵심 개념:
 * 1. Command (쓰기): 상태 변경 요청
 * 2. Query (읽기): 상태 조회 요청
 * 3. 분리: 두 모델을 완전히 독립적으로 관리
 * 4. 이벤트 기반 동기화: Command 실행 → Event 발행 → Query 모델 업데이트
 *
 * 장점:
 * - 성능: 읽기 모델을 독립적으로 최적화 가능
 * - 확장성: 읽기와 쓰기 스케일을 독립적으로 조정
 * - 복잡성: 비즈니스 로직을 읽기와 쓰기로 분리
 * - 이벤트: 자연스럽게 Event Sourcing과 결합
 *
 * 단점:
 * - 복잡함: 구현 난이도 높음
 * - 일관성: 최종 일관성(Eventual Consistency) 문제
 * - 모니터링: 동기화 지연 추적 필요
 *
 * 실무 사용 예:
 * - SNS: 포스트 작성(Command) vs 피드 조회(Query)
 * - 전자상거래: 주문(Command) vs 주문 목록(Query)
 * - 블로그: 글 작성(Command) vs 통계(Query)
 */
@Service
class CQRSService {
    private val log = LoggerFactory.getLogger(CQRSService::class.java)

    // ========================================================================
    // 1. Command Model (쓰기 모델)
    // ========================================================================
    /**
     * Command Model:
     * - 비즈니스 로직 중심
     * - 데이터 검증 및 규칙 적용
     * - 이벤트 발행
     * - 쓰기 최적화 (정규화된 데이터)
     *
     * 특징:
     * - 상태 변경만 수행
     * - 응답 속도는 중요하지 않음
     * - 일관성 최우선
     */

    private val commandStore = ConcurrentHashMap<String, OrderCommandModel>()

    data class OrderCommandModel(
        val orderId: String,
        val customerId: String,
        val totalAmount: Double,
        val status: OrderStatus,
        val createdAt: LocalDateTime,
        val lastUpdatedAt: LocalDateTime
    )

    /**
     * Command 1: 주문 생성
     *
     * 흐름:
     * 1. Command 수신
     * 2. 비즈니스 로직 검증
     * 3. 상태 변경 (Command Model)
     * 4. Event 발행 (Kafka)
     * 5. Query Model은 Event를 구독하여 자동 업데이트
     */
    fun createOrderCommand(command: CreateOrderCommand) {
        log.info(
            "[CQRS-CMD] 주문 생성 커맨드 처리 - orderId={}, customerId={}",
            command.orderId,
            command.customerId
        )

        // 1. 비즈니스 검증
        require(command.items.isNotEmpty()) { "상품이 최소 1개 필요" }
        require(command.totalAmount > 0) { "금액이 0보다 커야 함" }

        // 2. Command Model에 저장
        commandStore[command.orderId] = OrderCommandModel(
            orderId = command.orderId,
            customerId = command.customerId,
            totalAmount = command.totalAmount,
            status = OrderStatus.CREATED,
            createdAt = LocalDateTime.now(),
            lastUpdatedAt = LocalDateTime.now()
        )

        // 3. Event 발행
        val event = OrderCreatedEvent(
            orderId = command.orderId,
            customerId = command.customerId,
            totalAmount = command.totalAmount,
            items = command.items
        )
        log.info("[CQRS-CMD] Event 발행: OrderCreated - orderId={}", command.orderId)
        // kafkaProducerService.send("order-events", event)
    }

    /**
     * Command 2: 결제 처리
     */
    fun processPaymentCommand(command: ProcessPaymentCommand) {
        log.info(
            "[CQRS-CMD] 결제 처리 커맨드 - orderId={}, paymentId={}",
            command.orderId,
            command.paymentId
        )

        // 1. 현재 상태 확인
        val current = commandStore[command.orderId] ?: throw IllegalArgumentException("Order not found")
        require(current.status == OrderStatus.CREATED) { "Order must be in CREATED status" }

        // 2. Command Model 업데이트
        commandStore[command.orderId] = current.copy(
            status = OrderStatus.PAYMENT_COMPLETED,
            lastUpdatedAt = LocalDateTime.now()
        )

        // 3. Event 발행
        val event = PaymentCompletedEvent(
            paymentId = command.paymentId,
            orderId = command.orderId,
            amount = command.amount,
            paymentMethod = command.paymentMethod
        )
        log.info("[CQRS-CMD] Event 발행: PaymentCompleted - paymentId={}", command.paymentId)
        // kafkaProducerService.send("payment-events", event)
    }

    // ========================================================================
    // 2. Query Model (읽기 모델)
    // ========================================================================
    /**
     * Query Model:
     * - 읽기 최적화
     * - 역정규화된 데이터 (중복 허용)
     * - Event 기반 업데이트
     * - 실시간 조회 (또는 캐시)
     *
     * 특징:
     * - Command Model과 독립적
     * - 여러 개의 다른 쿼리 모델 존재 가능
     * - 조회 응답 속도 최우선
     * - 최종 일관성(Eventual Consistency) 허용
     */

    private val queryStore = ConcurrentHashMap<String, OrderQueryModel>()
    private val customerOrderIndexStore = ConcurrentHashMap<String, MutableList<String>>()

    /**
     * Query 1: 주문 상세 조회
     *
     * 특징:
     * - 매우 빠른 응답 (단순 HashMap 조회)
     * - 역정규화된 데이터 (결제, 배송 정보 포함)
     * - Query Model에서 제공
     */
    fun getOrderDetails(orderId: String): OrderQueryModel? {
        return queryStore[orderId].also {
            log.info("[CQRS-QRY] 주문 상세 조회 - orderId={}, status={}", orderId, it?.status)
        }
    }

    /**
     * Query 2: 고객의 모든 주문 조회
     *
     * 실무:
     * - 일반적으로 DB 인덱스로 구현
     * - 또는 Elasticsearch로 빠른 검색
     */
    fun getCustomerOrders(customerId: String): List<OrderQueryModel> {
        val orderIds = customerOrderIndexStore[customerId] ?: emptyList()
        return orderIds.mapNotNull { queryStore[it] }.also {
            log.info("[CQRS-QRY] 고객 주문 목록 조회 - customerId={}, 개수: {}", customerId, it.size)
        }
    }

    /**
     * Query 3: 주문 상태별 통계
     *
     * 실무:
     * - 대시보드용 데이터
     * - 미리 계산된 집계 데이터
     */
    fun getOrderStatistics(): Map<OrderStatus, Int> {
        return queryStore.values
            .groupingBy { it.status }
            .eachCount()
            .also {
                log.info("[CQRS-QRY] 주문 통계 조회 - {}", it)
            }
    }

    /**
     * Query 4: 고객별 매출 통계
     *
     * 실무: 분석 시스템용
     */
    fun getCustomerRevenueStatistics(): Map<String, Double> {
        return queryStore.values
            .groupingBy { it.customerId }
            .fold(0.0) { accumulator, element ->
                accumulator + element.totalAmount
            }
            .also {
                log.info("[CQRS-QRY] 고객별 매출 통계 - {}", it)
            }
    }

    // ========================================================================
    // 3. Event Handlers (Command Event → Query Model 동기화)
    // ========================================================================
    /**
     * Event Handler: OrderCreated
     *
     * 흐름:
     * 1. Kafka에서 OrderCreated 이벤트 수신
     * 2. Query Model 업데이트
     * 3. 인덱스 생성 (customerId → orderId)
     *
     * 중요: 이 과정은 비동기
     * - Command 실행 후 약간의 지연
     * - 최종 일관성(Eventual Consistency)
     */
    @KafkaListener(
        topics = ["order-events"],
        groupId = "cqrs-query-group-order-created"
    )
    fun handleOrderCreatedEvent(event: OrderCreatedEvent) {
        log.info(
            "[CQRS-HANDLER] OrderCreated Event 처리 - orderId={}, customerId={}",
            event.orderId,
            event.customerId
        )

        // 1. Query Model 생성
        val queryModel = OrderQueryModel(
            orderId = event.orderId,
            customerId = event.customerId,
            status = OrderStatus.CREATED,
            totalAmount = event.totalAmount,
            createdAt = event.createdAt,
            lastUpdatedAt = LocalDateTime.now()
        )

        queryStore[event.orderId] = queryModel

        // 2. Index 생성 (고객별 주문 목록)
        customerOrderIndexStore.computeIfAbsent(event.customerId) { mutableListOf() }
            .add(event.orderId)

        log.debug("[CQRS-HANDLER] Query Model 생성됨 - orderId={}", event.orderId)
    }

    /**
     * Event Handler: PaymentCompleted
     *
     * 흐름:
     * 1. Kafka에서 PaymentCompleted 이벤트 수신
     * 2. Query Model의 상태 업데이트
     */
    @KafkaListener(
        topics = ["payment-events"],
        groupId = "cqrs-query-group-payment-completed"
    )
    fun handlePaymentCompletedEvent(event: PaymentCompletedEvent) {
        log.info(
            "[CQRS-HANDLER] PaymentCompleted Event 처리 - orderId={}, paymentId={}",
            event.orderId,
            event.paymentId
        )

        // Query Model 업데이트
        queryStore.computeIfPresent(event.orderId) { _, existing ->
            existing.copy(
                status = OrderStatus.PAYMENT_COMPLETED,
                lastUpdatedAt = LocalDateTime.now()
            )
        }

        log.debug("[CQRS-HANDLER] Query Model 업데이트됨 - orderId={}", event.orderId)
    }

    /**
     * Event Handler: ShippingCompleted
     */
    @KafkaListener(
        topics = ["shipping-events"],
        groupId = "cqrs-query-group-shipping"
    )
    fun handleShippingCompletedEvent(event: ShippingCompletedEvent) {
        log.info(
            "[CQRS-HANDLER] ShippingCompleted Event 처리 - orderId={}, shippingId={}",
            event.orderId,
            event.shippingId
        )

        queryStore.computeIfPresent(event.orderId) { _, existing ->
            existing.copy(
                status = OrderStatus.DELIVERED,
                lastUpdatedAt = LocalDateTime.now()
            )
        }

        log.debug("[CQRS-HANDLER] Query Model 업데이트됨 - orderId={}", event.orderId)
    }

    // ========================================================================
    // 4. 모니터링: 동기화 지연 추적
    // ========================================================================
    /**
     * CQRS의 위험: 최종 일관성
     *
     * 문제:
     * - Command 실행 후 Query가 즉시 업데이트 안 됨
     * - 사용자가 이전 데이터를 볼 수 있음
     *
     * 해결책:
     * - 지연 시간 추적
     * - 임계값 초과 시 알림
     * - Read-after-write 일관성 보장
     */

    private data class SyncMetrics(
        val orderId: String,
        val commandTime: LocalDateTime,
        val eventPublishTime: LocalDateTime? = null,
        val queryUpdateTime: LocalDateTime? = null
    )

    private val syncMetrics = ConcurrentHashMap<String, SyncMetrics>()

    fun trackCommandExecution(orderId: String) {
        syncMetrics[orderId] = SyncMetrics(
            orderId = orderId,
            commandTime = LocalDateTime.now()
        )
    }

    fun trackEventPublish(orderId: String) {
        syncMetrics.computeIfPresent(orderId) { _, existing ->
            existing.copy(eventPublishTime = LocalDateTime.now())
        }
    }

    fun trackQueryUpdate(orderId: String) {
        syncMetrics.computeIfPresent(orderId) { _, existing ->
            val updateTime = LocalDateTime.now()
            existing.copy(queryUpdateTime = updateTime)?.let {
                val totalDelay = java.time.temporal.ChronoUnit.MILLIS
                    .between(it.commandTime, updateTime)

                log.info(
                    "[CQRS-METRICS] 동기화 지연 - orderId={}, delay={}ms",
                    orderId,
                    totalDelay
                )
            }
            existing.copy(queryUpdateTime = updateTime)
        }
    }

    // ========================================================================
    // 5. 멱등성 (Idempotency) - 중복 처리 방지
    // ========================================================================
    /**
     * Kafka 최소 보장(At-least-once) 특성:
     * - 메시지가 최소 1회 이상 전달됨
     * - 네트워크 오류 시 중복 가능
     *
     * 해결책: 멱등성
     * - 같은 요청을 여러 번 실행해도 결과 동일
     * - 예: 상태 변경은 멱등적 (이미 PAYMENT_COMPLETED면 무시)
     */

    private val processedEventIds = ConcurrentHashMap<String, LocalDateTime>()

    fun handleEventWithIdempotency(eventId: String, handler: () -> Unit) {
        if (processedEventIds.containsKey(eventId)) {
            log.warn("[CQRS-IDEMPOTENT] 이미 처리된 이벤트 - eventId={}", eventId)
            return
        }

        try {
            handler()
            processedEventIds[eventId] = LocalDateTime.now()
            log.debug("[CQRS-IDEMPOTENT] 이벤트 처리 완료 - eventId={}", eventId)
        } catch (e: Exception) {
            log.error("[CQRS-IDEMPOTENT] 이벤트 처리 실패 - eventId={}", eventId, e)
            throw e
        }
    }

    // ========================================================================
    // 6. 통합 예제: 온라인 스토어 CQRS
    // ========================================================================
    /**
     * 실무 시나리오:
     * 1. 사용자가 주문 생성 (Command)
     * 2. 시스템이 주문 목록 조회 (Query) - 약간의 지연 가능
     * 3. 관리자가 통계 조회 (Query) - 실시간성 낮아도 됨
     */

    fun executeOrderWorkflow() {
        // 1. Command: 주문 생성
        val createCmd = CreateOrderCommand(
            orderId = "order-001",
            customerId = "customer-001",
            items = listOf(
                OrderItem("prod-001", "노트북", 1, 1500000.0)
            ),
            totalAmount = 1500000.0
        )

        log.info("[CQRS-WORKFLOW] 1. 주문 생성 커맨드 실행")
        createOrderCommand(createCmd)

        // 2. Query: 주문 조회 (약간의 지연 발생 가능)
        Thread.sleep(100)  // 네트워크/이벤트 처리 지연 시뮬레이션

        log.info("[CQRS-WORKFLOW] 2. 주문 상세 조회")
        val order = getOrderDetails("order-001")
        log.info("[CQRS-WORKFLOW] 조회 결과: {}", order)

        // 3. Command: 결제 처리
        val paymentCmd = ProcessPaymentCommand(
            orderId = "order-001",
            paymentId = "payment-001",
            amount = 1500000.0,
            paymentMethod = "CREDIT_CARD"
        )

        log.info("[CQRS-WORKFLOW] 3. 결제 처리 커맨드 실행")
        processPaymentCommand(paymentCmd)

        // 4. Query: 고객의 모든 주문 조회
        Thread.sleep(100)

        log.info("[CQRS-WORKFLOW] 4. 고객 주문 목록 조회")
        val customerOrders = getCustomerOrders("customer-001")
        log.info("[CQRS-WORKFLOW] 고객 주문 수: {}", customerOrders.size)

        // 5. Query: 통계 조회
        log.info("[CQRS-WORKFLOW] 5. 주문 통계 조회")
        val stats = getOrderStatistics()
        log.info("[CQRS-WORKFLOW] 통계: {}", stats)
    }
}
