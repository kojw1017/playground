package com.example.playground.kafka.consumer

import com.example.playground.kafka.domain.*
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.retry.annotation.Backoff
import org.springframework.stereotype.Service
import java.time.LocalDateTime

/**
 * Kafka Consumer 학습 서비스
 *
 * 핵심 개념:
 * 1. 메시지 구독 (Subscribe)
 * 2. Consumer Group (분산 처리)
 * 3. Offset 관리 (자동 vs 수동 커밋)
 * 4. 에러 처리 및 재시도
 * 5. 배치 처리
 * 6. 메시지 헤더 처리
 */
@Service
class KafkaConsumerService {
    private val log = LoggerFactory.getLogger(KafkaConsumerService::class.java)

    // ========================================================================
    // 1. 기초: 단일 메시지 구독 (Single Message Listener)
    // ========================================================================
    /**
     * 기본 Consumer 구성:
     * - @KafkaListener: 특정 Topic을 구독
     * - groupId: Consumer Group 이름 (같은 Group의 여러 Consumer가 분산 처리)
     * - 자동 커밋 (enable-auto-commit: true)
     *
     * Consumer Group의 특징:
     * 1. 파티션 분배:
     *    - Consumer 1 → Partition 0
     *    - Consumer 2 → Partition 1
     *    - 각 Consumer는 할당된 Partition만 처리
     * 2. 로드 밸런싱: 시간이 지나면 자동으로 재분배
     * 3. 장애 복구: Consumer 충돌 → 다른 Consumer가 인수
     *
     * 자동 커밋 (Auto Commit):
     * - 주기: auto-commit-interval-ms (기본 1초)
     * - 장점: 간단함
     * - 단점: 메시지 처리 중 장애 → 중복 처리 가능
     */
    @KafkaListener(
        topics = ["order-events"],
        groupId = "order-service-group",  // Consumer Group
        concurrency = "3"                  // 3개 스레드로 병렬 처리
    )
    fun listenOrderEvents(
        @Payload event: OrderCreatedEvent,
        @Header("kafka_receivedPartitionId") partition: Int,
        @Header("kafka_offset") offset: Long
    ) {
        log.info(
            "[SINGLE] 주문 생성 이벤트 수신 - orderId={}, partition={}, offset={}",
            event.orderId,
            partition,
            offset
        )

        // 비즈니스 로직: 주문 처리
        try {
            processOrderEvent(event)
            log.debug("[SINGLE] 주문 처리 완료 - orderId={}", event.orderId)
        } catch (e: Exception) {
            log.error("[SINGLE] 주문 처리 실패 - orderId={}", event.orderId, e)
            throw e  // 예외 발생 시 자동 재시도 (RetryableTopic 필요)
        }
    }

    // ========================================================================
    // 2. 중급: 배치 처리 (Batch Listener)
    // ========================================================================
    /**
     * 배치 처리의 이점:
     * 1. 성능 향상: 한 번에 여러 메시지 처리
     * 2. DB 쿼리 최소화: 벌크 인서트/업데이트
     * 3. 트랜잭션 효율성: 배치 범위 내에서 원자성
     *
     * 설정:
     * - listener.type: batch
     * - max-poll-records: 500 (한 번에 가져올 메시지 수)
     *
     * 주의:
     * - 메모리 사용량 증가
     * - 처리 시간 증가 (긴 트랜잭션)
     */
    @KafkaListener(
        topics = ["payment-events"],
        groupId = "payment-service-group",
        concurrency = "2"
    )
    fun listenPaymentEventsBatch(
        @Payload events: List<PaymentCompletedEvent>
    ) {
        log.info(
            "[BATCH] 결제 완료 이벤트 배치 수신 - 개수: {}",
            events.size
        )

        try {
            // 배치 처리: 트랜잭션 한 번으로 여러 메시지 처리
            processPaymentBatch(events)
            log.info(
                "[BATCH] 결제 배치 처리 완료 - {}개 처리됨",
                events.size
            )
        } catch (e: Exception) {
            log.error("[BATCH] 결제 배치 처리 실패", e)
            throw e
        }
    }

    // ========================================================================
    // 3. 고급: 에러 처리 및 자동 재시도 (Retriable Topic)
    // ========================================================================
    /**
     * RetryableTopic:
     * 1. 실패한 메시지를 자동으로 retry topic으로 발행
     * 2. 지정된 횟수만큼 재시도
     * 3. 최종 실패 시 DLT(Dead Letter Topic)으로 전달
     *
     * 재시도 전략:
     * - Fixed Backoff: 1초, 1초, 1초 ...
     * - Exponential Backoff: 1초, 2초, 4초, 8초 ... (지수 증가)
     *
     * Topic 구조:
     * - shipping-events (원본)
     * - shipping-events-retry-0 (첫 번째 재시도)
     * - shipping-events-retry-1 (두 번째 재시도)
     * - shipping-events-dlt (최종 실패)
     */
    @RetryableTopic(
        attempts = "4",                    // 초기 시도 + 3회 재시도
        backoff = Backoff(
            delay = 1000,                  // 초기 지연: 1초
            multiplier = 2.0,              // 지수 백오프: 2배씩 증가
            maxDelay = 32000               // 최대 지연: 32초
        ),
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        retryTopicSuffix = "-retry",
        dltStrategy = org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR,
        dltTopicSuffix = "-dlt"
    )
    @KafkaListener(
        topics = ["shipping-events"],
        groupId = "shipping-service-group"
    )
    fun listenShippingEventsWithRetry(
        @Payload event: ShippingPreparedEvent,
        @Header(KafkaHeaders.DELIVERY_ATTEMPT) attempt: Int
    ) {
        log.info(
            "[RETRY] 배송 준비 이벤트 수신 (시도: {}) - shippingId={}, orderId={}",
            attempt,
            event.shippingId,
            event.orderId
        )

        try {
            processShippingEvent(event)
            log.debug("[RETRY] 배송 처리 완료 - shippingId={}", event.shippingId)
        } catch (e: Exception) {
            log.warn(
                "[RETRY] 배송 처리 실패, 재시도 예정 (시도: {}) - error: {}",
                attempt,
                e.message
            )
            throw e  // 재시도 로직이 자동으로 처리됨
        }
    }

    /**
     * Dead Letter Topic (DLT) Listener
     *
     * 용도:
     * - 최종 실패한 메시지 처리
     * - 모니터링 및 알림
     * - 수동 개입 필요
     *
     * 처리 방법:
     * 1. 로깅: 상세 정보 기록
     * 2. 알림: 슬랙, 이메일 발송
     * 3. DB 저장: 이후 수동 재처리
     */
    @KafkaListener(
        topics = ["shipping-events-dlt"],
        groupId = "shipping-dlt-group"
    )
    fun handleShippingDLT(
        @Payload event: ShippingPreparedEvent,
        @Header(KafkaHeaders.EXCEPTION_MESSAGE) exceptionMessage: String
    ) {
        log.error(
            "[DLT] 배송 이벤트 최종 실패 - shippingId={}, error={}",
            event.shippingId,
            exceptionMessage
        )

        // 실무: 외부 알림 시스템에 통보
        // notificationService.sendAlert(...)

        // 실무: DLT 메시지를 DB에 저장하여 나중에 재처리
        // dlMessageRepository.save(...)
    }

    // ========================================================================
    // 4. 고급: 메시지 헤더 및 메타데이터 처리
    // ========================================================================
    /**
     * Kafka 메시지 헤더 활용:
     * 1. 이벤트 추적 (Traceability)
     * 2. 상관관계 ID 추출
     * 3. 시간 정보 기록
     * 4. 소스 시스템 확인
     */
    @KafkaListener(
        topics = ["order-events"],
        groupId = "analytics-group"
    )
    fun listenOrderEventsWithHeaders(
        @Payload event: OrderCreatedEvent,
        @Header("kafka_receivedTopic") topic: String,
        @Header("kafka_receivedPartitionId") partition: Int,
        @Header("kafka_offset") offset: Long,
        @Header("eventId") eventId: String?,
        @Header("correlationId") correlationId: String?,
        @Header("source") source: String?
    ) {
        log.info(
            "[HEADERS] 주문 이벤트 수신 - orderId={}, " +
            "topic={}, partition={}, offset={}, " +
            "eventId={}, correlationId={}, source={}",
            event.orderId,
            topic,
            partition,
            offset,
            eventId,
            correlationId,
            source
        )

        // 분석 데이터 수집
        recordAnalytics(
            eventId = eventId ?: "unknown",
            correlationId = correlationId ?: "unknown",
            source = source ?: "unknown",
            topic = topic,
            partition = partition,
            offset = offset,
            timestamp = LocalDateTime.now()
        )
    }

    // ========================================================================
    // 5. 고급: 수동 커밋 (Manual Commit)
    // ========================================================================
    /**
     * 수동 커밋 vs 자동 커밋:
     *
     * 자동 커밋:
     * - 장점: 간단함
     * - 단점: 메시지 처리 중 실패 → 중복 처리 가능
     *
     * 수동 커밋:
     * - 장점: 정확한 처리 보장
     * - 단점: 복잡함
     * - 필요: enable-auto-commit: false 설정
     *
     * 사용 시기:
     * - 정확히 한 번(Exactly-once) 처리 필요
     * - DB 트랜잭션과 조화 필요
     *
     * 주의:
     * - 수동 커밋 실패 시 Consumer 재시작 → 중복 처리
     * - 따라서 Idempotent 처리 필요 (멱등성)
     */
    @KafkaListener(
        topics = ["critical-events"],
        groupId = "critical-events-group"
    )
    fun listenCriticalEventsWithManualCommit(
        @Payload event: OrderCreatedEvent,
        acknowledgment: org.springframework.kafka.support.Acknowledgment?
    ) {
        log.info(
            "[MANUAL] 중요 이벤트 수신 (수동 커밋) - orderId={}",
            event.orderId
        )

        try {
            // 메시지 처리
            processCriticalEvent(event)

            // ✓ 성공 시에만 커밋
            acknowledgment?.acknowledge()
            log.debug("[MANUAL] 메시지 커밋됨 - orderId={}", event.orderId)

        } catch (e: Exception) {
            log.error("[MANUAL] 메시지 처리 실패, 커밋 스킵 - orderId={}", event.orderId, e)
            // ✗ 실패하면 커밋 안 함 → Offset 유지 → 다음 poll에서 재처리
            throw e
        }
    }

    // ========================================================================
    // 6. 통합: 프로덕션 레벨 Consumer
    // ========================================================================
    /**
     * 프로덕션 레벨의 Consumer 구성:
     * 1. 배치 처리로 성능 향상
     * 2. 헤더 기반 메타데이터 추출
     * 3. 자동 재시도
     * 4. 예외 처리
     * 5. 로깅 및 모니터링
     */
    @RetryableTopic(
        attempts = "3",
        backoff = Backoff(delay = 1000, multiplier = 2.0),
        dltTopicSuffix = "-dlt"
    )
    @KafkaListener(
        topics = ["production-events"],
        groupId = "production-service-group",
        concurrency = "5"
    )
    fun listenProductionEvents(
        @Payload events: List<OrderCreatedEvent>,
        @Header("kafka_receivedPartitionId") partition: Int,
        @Header("kafka_deliveryAttempt") attempt: Int
    ) {
        log.info(
            "[PRODUCTION] 프로덕션 이벤트 배치 수신 - " +
            "개수: {}, partition: {}, attempt: {}",
            events.size,
            partition,
            attempt
        )

        try {
            // 배치 처리
            val startTime = System.currentTimeMillis()

            processProductionBatch(events)

            val duration = System.currentTimeMillis() - startTime
            log.info(
                "[PRODUCTION] 배치 처리 완료 - {}개 처리, {}ms 소요",
                events.size,
                duration
            )

        } catch (e: Exception) {
            log.error(
                "[PRODUCTION] 배치 처리 실패 - 개수: {}, attempt: {}, error: {}",
                events.size,
                attempt,
                e.message,
                e
            )
            throw e  // 재시도 로직에 위임
        }
    }

    // ========================================================================
    // 비즈니스 로직 (실제 구현은 별도 Service로 분리)
    // ========================================================================

    private fun processOrderEvent(event: OrderCreatedEvent) {
        // 실제 구현: 주문 DB 저장, 결제 서비스 호출 등
        Thread.sleep(100)  // 시뮬레이션
    }

    private fun processPaymentBatch(events: List<PaymentCompletedEvent>) {
        // 실제 구현: 배치 결제 처리
        Thread.sleep(100)  // 시뮬레이션
    }

    private fun processShippingEvent(event: ShippingPreparedEvent) {
        // 실제 구현: 배송 준비 처리
        Thread.sleep(100)  // 시뮬레이션
    }

    private fun processCriticalEvent(event: OrderCreatedEvent) {
        // 실제 구현: 중요한 주문 처리
        Thread.sleep(100)  // 시뮬레이션
    }

    private fun processProductionBatch(events: List<OrderCreatedEvent>) {
        // 실제 구현: 대량 주문 처리
        Thread.sleep(100)  // 시뮬레이션
    }

    private fun recordAnalytics(
        eventId: String,
        correlationId: String,
        source: String,
        topic: String,
        partition: Int,
        offset: Long,
        timestamp: LocalDateTime
    ) {
        // 실제 구현: 분석 데이터 수집
        log.debug(
            "[ANALYTICS] 기록됨 - eventId={}, source={}, topic={}",
            eventId,
            source,
            topic
        )
    }
}
