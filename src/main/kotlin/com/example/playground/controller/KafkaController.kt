package com.example.playground.controller

import com.example.playground.kafka.domain.*
import com.example.playground.kafka.producer.KafkaProducerService
import com.example.playground.kafka.patterns.eventsourcing.EventSourcingService
import com.example.playground.kafka.patterns.cqrs.CQRSService
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.LocalDateTime
import java.util.UUID

/**
 * Kafka 학습 통합 API
 *
 * 이 컨트롤러는 모든 Kafka 학습 내용을 REST API로 제공합니다.
 * Postman이나 cURL로 쉽게 테스트할 수 있습니다.
 *
 * 구성:
 * 1. Producer 예제
 * 2. Event Sourcing 예제
 * 3. CQRS 예제
 */
@RestController
@RequestMapping("/api/kafka")
class KafkaController(
    private val producerService: KafkaProducerService,
    private val eventSourcingService: EventSourcingService,
    private val cqrsService: CQRSService
) {
    private val log = LoggerFactory.getLogger(KafkaController::class.java)

    // ========================================================================
    // 1. Producer 예제
    // ========================================================================

    /**
     * 동기 전송 예제
     *
     * POST /api/kafka/producer/sync
     *
     * 요청 본문:
     * {
     *   "customerId": "cust-001",
     *   "items": [
     *     {
     *       "productId": "prod-001",
     *       "productName": "노트북",
     *       "quantity": 1,
     *       "price": 1500000
     *     }
     *   ],
     *   "totalAmount": 1500000
     * }
     */
    @PostMapping("/producer/sync")
    fun sendMessageSync(
        @RequestBody request: CreateOrderRequest
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] 동기 메시지 전송 요청 - customerId={}", request.customerId)

        val orderId = UUID.randomUUID().toString()

        val event = OrderCreatedEvent(
            orderId = orderId,
            customerId = request.customerId,
            totalAmount = request.totalAmount,
            items = request.items
        )

        val success = producerService.sendOrderEventSync(event)

        return ResponseEntity.ok(mapOf(
            "success" to success,
            "orderId" to orderId,
            "message" to "동기 메시지 전송 완료"
        ))
    }

    /**
     * 비동기 전송 예제
     *
     * POST /api/kafka/producer/async
     */
    @PostMapping("/producer/async")
    fun sendMessageAsync(
        @RequestBody request: PaymentRequest
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] 비동기 메시지 전송 요청 - orderId={}", request.orderId)

        val event = PaymentCompletedEvent(
            paymentId = UUID.randomUUID().toString(),
            orderId = request.orderId,
            amount = request.amount,
            paymentMethod = request.paymentMethod
        )

        producerService.sendPaymentEventAsync(event)

        return ResponseEntity.ok(mapOf(
            "message" to "비동기 메시지 전송 요청됨 (결과는 나중에 콜백으로 처리)",
            "orderId" to request.orderId
        ))
    }

    /**
     * 배치 전송 예제
     *
     * POST /api/kafka/producer/batch
     */
    @PostMapping("/producer/batch")
    fun sendBatchMessages(
        @RequestBody request: BatchOrderRequest
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] 배치 메시지 전송 요청 - 개수: {}", request.count)

        val orders = (1..request.count).map { index ->
            OrderCreatedEvent(
                orderId = "order-batch-${System.currentTimeMillis()}-$index",
                customerId = "customer-${index % 10}",
                totalAmount = (100000 * index).toDouble(),
                items = listOf(
                    OrderItem(
                        productId = "product-$index",
                        productName = "상품 $index",
                        quantity = 1,
                        price = (100000 * index).toDouble()
                    )
                )
            )
        }

        producerService.sendBatchOrders(orders)

        return ResponseEntity.ok(mapOf(
            "message" to "배치 메시지 전송 완료",
            "count" to request.count,
            "totalAmount" to orders.sumOf { it.totalAmount }
        ))
    }

    /**
     * 메타데이터 포함 전송 예제
     *
     * POST /api/kafka/producer/with-headers
     */
    @PostMapping("/producer/with-headers")
    fun sendWithHeaders(
        @RequestBody request: CreateOrderRequest
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] 메타데이터 포함 메시지 전송 - customerId={}", request.customerId)

        val orderId = UUID.randomUUID().toString()
        val correlationId = UUID.randomUUID().toString()

        val event = OrderCreatedEvent(
            orderId = orderId,
            customerId = request.customerId,
            totalAmount = request.totalAmount,
            items = request.items
        )

        producerService.sendEventWithHeaders(orderId, event, correlationId)

        return ResponseEntity.ok(mapOf(
            "message" to "메타데이터 포함 메시지 전송 완료",
            "orderId" to orderId,
            "correlationId" to correlationId
        ))
    }

    // ========================================================================
    // 2. Event Sourcing 예제
    // ========================================================================

    /**
     * Event Sourcing: 주문 생성
     *
     * POST /api/kafka/event-sourcing/create-order
     */
    @PostMapping("/event-sourcing/create-order")
    fun createOrderES(
        @RequestBody request: CreateOrderRequest
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] Event Sourcing 주문 생성 - customerId={}", request.customerId)

        val orderId = UUID.randomUUID().toString()

        val command = CreateOrderCommand(
            orderId = orderId,
            customerId = request.customerId,
            items = request.items,
            totalAmount = request.totalAmount
        )

        eventSourcingService.createOrder(command)

        return ResponseEntity.ok(mapOf(
            "message" to "주문 생성 이벤트 저장됨",
            "orderId" to orderId
        ))
    }

    /**
     * Event Sourcing: 주문 상태 조회
     *
     * GET /api/kafka/event-sourcing/order/{orderId}
     */
    @GetMapping("/event-sourcing/order/{orderId}")
    fun getOrderStateES(
        @PathVariable orderId: String
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] Event Sourcing 주문 상태 조회 - orderId={}", orderId)

        val state = eventSourcingService.reconstructOrderState(orderId)

        return if (state != null) {
            ResponseEntity.ok(mapOf(
                "orderId" to orderId,
                "status" to state.status,
                "customerId" to state.customerId,
                "totalAmount" to state.totalAmount,
                "createdAt" to state.createdAt,
                "lastUpdatedAt" to state.lastUpdatedAt
            ))
        } else {
            ResponseEntity.notFound().build()
        }
    }

    /**
     * Event Sourcing: 주문 히스토리 조회
     *
     * GET /api/kafka/event-sourcing/order/{orderId}/history
     */
    @GetMapping("/event-sourcing/order/{orderId}/history")
    fun getOrderHistoryES(
        @PathVariable orderId: String
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] Event Sourcing 주문 히스토리 조회 - orderId={}", orderId)

        val timeline = eventSourcingService.getOrderTimeline(orderId)

        return ResponseEntity.ok(mapOf(
            "orderId" to orderId,
            "timeline" to timeline,
            "totalEvents" to timeline.size
        ))
    }

    /**
     * Event Sourcing: 시간 여행
     *
     * GET /api/kafka/event-sourcing/order/{orderId}/at-time?time=2026-01-26T10:00:00
     */
    @GetMapping("/event-sourcing/order/{orderId}/at-time")
    fun getOrderStateAtTimeES(
        @PathVariable orderId: String,
        @RequestParam time: String
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] Event Sourcing 시간 여행 - orderId={}, time={}", orderId, time)

        val targetTime = LocalDateTime.parse(time)
        val state = eventSourcingService.getOrderStateAtTime(orderId, targetTime)

        return if (state != null) {
            ResponseEntity.ok(mapOf(
                "orderId" to orderId,
                "targetTime" to time,
                "status" to state.status,
                "message" to "해당 시점의 주문 상태"
            ))
        } else {
            ResponseEntity.notFound().build()
        }
    }

    /**
     * Event Sourcing: 통계
     *
     * GET /api/kafka/event-sourcing/statistics
     */
    @GetMapping("/event-sourcing/statistics")
    fun getEventSourcingStatistics(): ResponseEntity<Map<String, Any>> {
        log.info("[API] Event Sourcing 통계 조회")

        val stats = eventSourcingService.getOrderStatistics()

        return ResponseEntity.ok(mapOf(
            "totalEvents" to stats.totalEvents,
            "ordersCreated" to stats.ordersCreated,
            "paymentsProcessed" to stats.paymentsProcessed,
            "ordersShipped" to stats.ordersShipped
        ))
    }

    // ========================================================================
    // 3. CQRS 예제
    // ========================================================================

    /**
     * CQRS: Command - 주문 생성
     *
     * POST /api/kafka/cqrs/create-order
     */
    @PostMapping("/cqrs/create-order")
    fun createOrderCQRS(
        @RequestBody request: CreateOrderRequest
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] CQRS 주문 생성 커맨드 - customerId={}", request.customerId)

        val orderId = UUID.randomUUID().toString()

        val command = CreateOrderCommand(
            orderId = orderId,
            customerId = request.customerId,
            items = request.items,
            totalAmount = request.totalAmount
        )

        cqrsService.createOrderCommand(command)

        return ResponseEntity.ok(mapOf(
            "message" to "주문 생성 커맨드 처리됨",
            "orderId" to orderId,
            "note" to "Query 모델은 이벤트 처리 후 업데이트됨 (최종 일관성)"
        ))
    }

    /**
     * CQRS: Query - 주문 상세 조회
     *
     * GET /api/kafka/cqrs/order/{orderId}
     */
    @GetMapping("/cqrs/order/{orderId}")
    fun getOrderDetailsCQRS(
        @PathVariable orderId: String
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] CQRS 주문 상세 조회 - orderId={}", orderId)

        val order = cqrsService.getOrderDetails(orderId)

        return if (order != null) {
            ResponseEntity.ok(mapOf(
                "orderId" to orderId,
                "customerId" to order.customerId,
                "status" to order.status,
                "totalAmount" to order.totalAmount,
                "createdAt" to order.createdAt,
                "lastUpdatedAt" to order.lastUpdatedAt
            ))
        } else {
            ResponseEntity.notFound().build()
        }
    }

    /**
     * CQRS: Query - 고객의 모든 주문 조회
     *
     * GET /api/kafka/cqrs/customer/{customerId}/orders
     */
    @GetMapping("/cqrs/customer/{customerId}/orders")
    fun getCustomerOrdersCQRS(
        @PathVariable customerId: String
    ): ResponseEntity<Map<String, Any>> {
        log.info("[API] CQRS 고객 주문 목록 조회 - customerId={}", customerId)

        val orders = cqrsService.getCustomerOrders(customerId)

        return ResponseEntity.ok(mapOf(
            "customerId" to customerId,
            "orders" to orders.map { order ->
                mapOf(
                    "orderId" to order.orderId,
                    "status" to order.status,
                    "totalAmount" to order.totalAmount
                )
            },
            "totalOrders" to orders.size
        ))
    }

    /**
     * CQRS: Query - 주문 통계
     *
     * GET /api/kafka/cqrs/statistics
     */
    @GetMapping("/cqrs/statistics")
    fun getOrderStatisticsCQRS(): ResponseEntity<Map<String, Any>> {
        log.info("[API] CQRS 주문 통계 조회")

        val stats = cqrsService.getOrderStatistics()
        val revenue = cqrsService.getCustomerRevenueStatistics()

        return ResponseEntity.ok(mapOf(
            "statistics" to stats,
            "revenue" to revenue
        ))
    }

    // ========================================================================
    // 4. 통합 워크플로우 예제
    // ========================================================================

    /**
     * 전체 워크플로우 실행
     *
     * POST /api/kafka/workflow/execute
     *
     * 이 엔드포인트는:
     * 1. 주문 생성 (CQRS Command)
     * 2. 주문 조회 (CQRS Query)
     * 3. 결제 처리 (CQRS Command)
     * 4. 통계 조회 (CQRS Query)
     * 를 모두 실행합니다.
     */
    @PostMapping("/workflow/execute")
    fun executeWorkflow(): ResponseEntity<Map<String, Any>> {
        log.info("[API] Kafka 워크플로우 실행 시작")

        cqrsService.executeOrderWorkflow()

        return ResponseEntity.ok(mapOf(
            "message" to "Kafka 워크플로우 실행 완료",
            "steps" to listOf(
                "1. 주문 생성 (Command)",
                "2. 주문 조회 (Query)",
                "3. 결제 처리 (Command)",
                "4. 고객 주문 목록 조회 (Query)",
                "5. 통계 조회 (Query)"
            ),
            "note" to "로그를 통해 각 단계를 확인하세요"
        ))
    }

    // ========================================================================
    // 5. 헬스 체크 및 정보
    // ========================================================================

    /**
     * Kafka 학습 API 정보
     *
     * GET /api/kafka/info
     */
    @GetMapping("/info")
    fun getKafkaInfo(): ResponseEntity<Map<String, Any>> {
        return ResponseEntity.ok(mapOf(
            "name" to "Kafka 속성 교육 API",
            "version" to "1.0.0",
            "topics" to listOf(
                "order-events",
                "payment-events",
                "shipping-events",
                "critical-events",
                "production-events"
            ),
            "patterns" to listOf(
                "Event Sourcing",
                "CQRS"
            ),
            "endpoints" to mapOf(
                "producer" to "/api/kafka/producer/**",
                "event-sourcing" to "/api/kafka/event-sourcing/**",
                "cqrs" to "/api/kafka/cqrs/**",
                "workflow" to "/api/kafka/workflow/**"
            )
        ))
    }

    // ========================================================================
    // 요청/응답 DTO
    // ========================================================================

    data class CreateOrderRequest(
        val customerId: String,
        val items: List<OrderItem>,
        val totalAmount: Double
    )

    data class PaymentRequest(
        val orderId: String,
        val amount: Double,
        val paymentMethod: String
    )

    data class BatchOrderRequest(
        val count: Int
    )
}
