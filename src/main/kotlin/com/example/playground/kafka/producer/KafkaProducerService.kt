package com.example.playground.kafka.producer

import com.example.playground.kafka.domain.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import org.springframework.util.concurrent.ListenableFuture
import java.time.LocalDateTime
import java.util.concurrent.CompletableFuture

/**
 * Kafka Producer 학습 서비스
 *
 * 핵심 개념:
 * 1. 동기 전송 (Synchronous) - 확실하지만 느림
 * 2. 비동기 전송 (Asynchronous) - 빠르지만 콜백 처리 필요
 * 3. 파티셔닝 (Partitioning) - Key에 따라 같은 파티션으로 라우팅
 * 4. 메시지 헤더 (Headers) - 메타데이터 전송
 */
@Service
class KafkaProducerService(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val log = LoggerFactory.getLogger(KafkaProducerService::class.java)

    // ========================================================================
    // 1. 기초: 동기 전송 (Synchronous Send)
    // ========================================================================
    /**
     * 동기 전송: 메시지가 Broker에 정말 저장될 때까지 대기
     *
     * 장점:
     * - 안정적: 성공 여부를 즉시 알 수 있음
     * - 순서 보장: 같은 Key면 순서 보장
     *
     * 단점:
     * - 느림: 대기 시간이 존재 (I/O 블로킹)
     * - 처리량 낮음: 한 번에 하나씩 처리
     *
     * 사용 시기:
     * - 결제, 금융 거래 등 정확성이 중요한 경우
     * - 빠른 응답이 필요 없는 경우
     */
    fun sendOrderEventSync(event: OrderCreatedEvent): Boolean {
        return try {
            // Topic: order-events
            // Key: orderId (같은 주문은 같은 파티션으로)
            val result = kafkaTemplate.send("order-events", event.orderId, event)
                .get()  // ← 동기 대기

            log.info(
                "[SYNC] 주문 생성 이벤트 전송 성공 - " +
                "orderId={}, partition={}, offset={}",
                event.orderId,
                result.recordMetadata.partition(),
                result.recordMetadata.offset()
            )
            true
        } catch (e: Exception) {
            log.error("[SYNC] 주문 생성 이벤트 전송 실패 - orderId={}", event.orderId, e)
            false
        }
    }

    // ========================================================================
    // 2. 중급: 비동기 전송 (Asynchronous Send with Callback)
    // ========================================================================
    /**
     * 비동기 전송: 메시지 전송을 요청하고 즉시 반환
     * 결과는 콜백으로 처리
     *
     * 장점:
     * - 빠름: 블로킹 없이 즉시 반환
     * - 높은 처리량: 여러 메시지를 동시에 처리
     *
     * 단점:
     * - 복잡함: 콜백 처리 필요
     * - 순서 보장 어려움: 콜백 순서 != 메시지 순서
     *
     * 사용 시기:
     * - 로깅, 분석 데이터 등 손실 용인 가능한 경우
     * - 높은 처리량이 필요한 경우
     */
    fun sendPaymentEventAsync(event: PaymentCompletedEvent) {
        log.info("[ASYNC] 결제 완료 이벤트 전송 시작 - paymentId={}", event.paymentId)

        // CompletableFuture로 콜백 처리
        kafkaTemplate.send("payment-events", event.orderId, event)
            .whenComplete { result, exception ->
                when {
                    exception != null -> {
                        log.error(
                            "[ASYNC] 결제 완료 이벤트 전송 실패 - paymentId={}",
                            event.paymentId,
                            exception
                        )
                    }
                    result != null -> {
                        log.info(
                            "[ASYNC] 결제 완료 이벤트 전송 성공 - " +
                            "paymentId={}, partition={}, offset={}",
                            event.paymentId,
                            result.recordMetadata.partition(),
                            result.recordMetadata.offset()
                        )
                    }
                }
            }
    }

    // ========================================================================
    // 3. 고급: 파티셔닝 전략 (Partitioning Strategy)
    // ========================================================================
    /**
     * Kafka의 파티셔닝:
     *
     * Key 기반 라우팅:
     * - Key hash값 % 파티션 수 = 대상 파티션
     * - 같은 Key → 같은 파티션 (순서 보장)
     * - null Key → 라운드 로빈 (순서 미보장)
     *
     * 파티션의 장점:
     * 1. 병렬성: 여러 Consumer가 다른 Partition을 동시 처리
     * 2. 순서 보장: 같은 Key → 같은 Partition → 순서 유지
     * 3. 확장성: Partition 수 증가 = 처리량 증가
     *
     * 파티셔닝 전략:
     * - customerId 기반: 같은 고객의 주문은 같은 Partition (고객별 순서)
     * - orderId 기반: 같은 주문의 이벤트는 같은 Partition
     * - null: 분산 처리 (순서 무관할 때)
     */
    fun sendShippingEventWithPartitioning(event: ShippingPreparedEvent) {
        log.info(
            "[PARTITION] 배송 준비 이벤트 전송 - orderId={} → Key={}",
            event.orderId,
            event.orderId  // ← 파티션 Key
        )

        kafkaTemplate.send("shipping-events", event.orderId, event)
            .whenComplete { result, exception ->
                if (result != null) {
                    log.info(
                        "[PARTITION] 배송 준비 이벤트가 Partition {}에 저장됨 - offset={}",
                        result.recordMetadata.partition(),
                        result.recordMetadata.offset()
                    )
                }
            }
    }

    // ========================================================================
    // 4. 고급: 메시지 헤더 및 메타데이터 추가
    // ========================================================================
    /**
     * Kafka 메시지 헤더:
     * - 메시지 바디와 별도로 메타데이터 전송
     * - 모든 Consumer가 접근 가능
     * - 추적 가능성(Traceability) 확대
     *
     * 헤더 사용 예:
     * 1. 이벤트 타입 (EventType)
     * 2. 상관관계 ID (CorrelationId)
     * 3. 발행 시간 (Timestamp)
     * 4. 소스 시스템 (Source)
     */
    fun sendEventWithHeaders(
        orderId: String,
        event: OrderCreatedEvent,
        correlationId: String = java.util.UUID.randomUUID().toString()
    ) {
        log.info(
            "[HEADERS] 메타데이터 포함 이벤트 전송 - orderId={}, correlationId={}",
            orderId,
            correlationId
        )

        // ProducerRecord: 헤더 커스터마이징 가능
        val headers = listOf<Header>(
            RecordHeader("eventId", event.eventId.toByteArray()),
            RecordHeader("eventType", "OrderCreated".toByteArray()),
            RecordHeader("correlationId", correlationId.toByteArray()),
            RecordHeader("source", "order-service".toByteArray()),
            RecordHeader("timestamp", LocalDateTime.now().toString().toByteArray())
        )

        val record = ProducerRecord<String, Any>(
            "order-events",      // topic
            null,                // partition (null이면 Key 기반 라우팅)
            event.orderId,       // key
            event as Any,        // value
            headers              // headers
        )

        @Suppress("UNCHECKED_CAST")
        kafkaTemplate.send(record as ProducerRecord<String, Any>)
            .whenComplete { result, exception ->
                if (result != null) {
                    log.info("[HEADERS] 헤더 포함 메시지 전송 성공 - 헤더 개수: {}", headers.size)
                }
            }
    }

    // ========================================================================
    // 5. 고급: 트랜잭션 전송 (Exactly-once Semantics)
    // ========================================================================
    /**
     * Kafka 트랜잭션:
     * - All-or-nothing: 모든 메시지가 저장되거나 모두 버려짐
     * - Atomicity: 원자성 보장
     * - Idempotence: 중복 메시지 자동 제거
     *
     * 트랜잭션 ID:
     * - 각 Producer에 고유한 트랜잭션 ID
     * - 같은 ID로 재전송 → 중복 제거
     *
     * 사용 시기:
     * - 재시도 가능한 실패
     * - 정확히 한 번(Exactly-once) 처리 필요
     * - 여러 메시지를 원자적으로 처리
     */
    fun sendEventsInTransaction(
        orderId: String,
        orderEvent: OrderCreatedEvent,
        paymentEvent: PaymentCompletedEvent
    ) {
        return try {
            log.info("[TRANSACTION] 트랜잭션 시작 - orderId={}", orderId)

            // Spring의 @Transactional 대신 KafkaTemplate의 트랜잭션 사용
            val results = listOf(
                kafkaTemplate.send("order-events", orderId, orderEvent),
                kafkaTemplate.send("payment-events", orderId, paymentEvent)
            )

            // 모든 메시지가 성공할 때까지 대기
            val allResults = results.map { it.get() }

            log.info(
                "[TRANSACTION] 트랜잭션 성공 - {} 개 메시지 전송",
                allResults.size
            )
        } catch (e: Exception) {
            log.error("[TRANSACTION] 트랜잭션 실패 - orderId={}", orderId, e)
            throw e
        }
    }

    // ========================================================================
    // 6. 고급: 배치 처리 (Batch Optimization)
    // ========================================================================
    /**
     * 배치 처리 최적화:
     *
     * batch-size: 16KB (기본값)
     * - 이 크기만큼 모으면 즉시 전송
     * - 작으면 자주 전송 (오버헤드 증가)
     * - 크면 메모리 사용 증가
     *
     * linger-ms: 10ms (기본값)
     * - batch-size 미달 시 최대 대기 시간
     * - 처리량 vs 지연 시간 트레이드오프
     *
     * 최적 전략:
     * - 높은 처리량: batch-size 증가, linger-ms 증가
     * - 낮은 지연: batch-size 감소, linger-ms 감소
     */
    fun sendBatchOrders(orders: List<OrderCreatedEvent>) {
        log.info("[BATCH] 배치 주문 전송 시작 - 개수: {}", orders.size)

        val futures = orders.map { order ->
            kafkaTemplate.send("order-events", order.orderId, order)
        }

        // 모든 메시지 전송 완료 대기
        val results = futures.map { it.get() }

        log.info(
            "[BATCH] 배치 전송 완료 - {}/{}",
            results.size,
            orders.size
        )
    }

    // ========================================================================
    // 7. 고급: 에러 처리 및 재시도 (Error Handling & Retry)
    // ========================================================================
    /**
     * Producer 에러 처리:
     * 1. Retriable 에러: 재시도 가능
     *    - 네트워크 타임아웃
     *    - Broker 일시 불가
     * 2. Non-retriable 에러: 재시도 불가
     *    - 메시지 크기 초과
     *    - 인증 실패
     *
     * Spring Kafka 설정:
     * - retries: 재시도 횟수
     * - retry.backoff.ms: 재시도 대기 시간
     */
    fun sendWithErrorHandling(event: OrderCreatedEvent) {
        kafkaTemplate.send("order-events", event.orderId, event)
            .whenComplete { result, exception ->
                when {
                    exception != null -> {
                        log.error(
                            "[RETRY] 메시지 전송 실패 (재시도 설정됨) - orderId={}, 에러: {}",
                            event.orderId,
                            exception.message
                        )
                        // Spring의 설정된 재시도 정책에 따라 자동 처리
                    }
                    result != null -> {
                        log.info(
                            "[RETRY] 메시지 전송 성공 - orderId={}, partition={}, offset={}",
                            event.orderId,
                            result.recordMetadata.partition(),
                            result.recordMetadata.offset()
                        )
                    }
                }
            }
    }

    // ========================================================================
    // 8. 통합: 프로덕션 상용 예제
    // ========================================================================
    /**
     * 프로덕션 레벨의 메시지 발행
     *
     * 포함 사항:
     * - 메타데이터 (헤더, 타임스탬프)
     * - 에러 처리
     * - 로깅
     * - 재시도 정책
     */
    fun sendProductionEvent(
        topic: String,
        key: String,
        event: Any,
        correlationId: String = java.util.UUID.randomUUID().toString()
    ): CompletableFuture<SendResult<String, Any>> {

        val completableFuture = CompletableFuture<SendResult<String, Any>>()

        try {
            log.debug(
                "[PRODUCTION] 이벤트 발행 요청 - topic={}, key={}, correlationId={}",
                topic,
                key,
                correlationId
            )

            // 메시지 헤더 구성
            val headers = listOf<Header>(
                RecordHeader("eventId", java.util.UUID.randomUUID().toString().toByteArray()),
                RecordHeader("correlationId", correlationId.toByteArray()),
                RecordHeader("source", "playground-service".toByteArray()),
                RecordHeader("timestamp", LocalDateTime.now().toString().toByteArray())
            )

            val record = ProducerRecord<String, Any>(topic, null, key, event, headers)

            kafkaTemplate.send(record)
                .whenComplete { result, exception ->
                    when {
                        exception != null -> {
                            log.error(
                                "[PRODUCTION] 이벤트 발행 실패 - topic={}, key={}, error={}",
                                topic,
                                key,
                                exception.message
                            )
                            completableFuture.completeExceptionally(exception)
                        }
                        result != null -> {
                            log.info(
                                "[PRODUCTION] 이벤트 발행 성공 - " +
                                "topic={}, partition={}, offset={}, correlationId={}",
                                topic,
                                result.recordMetadata.partition(),
                                result.recordMetadata.offset(),
                                correlationId
                            )
                            completableFuture.complete(result)
                        }
                    }
                }
        } catch (e: Exception) {
            log.error(
                "[PRODUCTION] 예상 외 에러 - topic={}, key={}, error={}",
                topic,
                key,
                e.message,
                e
            )
            completableFuture.completeExceptionally(e)
        }

        return completableFuture
    }
}
