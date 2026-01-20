package com.example.playground

import com.example.playground.service.TransactionPracticeService
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class Runner(private val transactionPracticeService: TransactionPracticeService) {
    private val log = LoggerFactory.getLogger(javaClass)

    @EventListener(ApplicationReadyEvent::class)
    fun onReady() {
        log.info("Runner: calling TransactionPracticeService.findByAndModify if possible (no DB id may exist)")
        try {
            // 안전하게 호출: 존재하지 않는 id로 예외가 발생할 수 있으니 로그 호출 메서드로 대체
            transactionPracticeService.externalCall("aop-test-team")
        } catch (ex: Exception) {
            log.warn("Runner: service call threw: ${ex.message}")
        }

        log.info("Runner: calling saveTeamRuntimeException to demonstrate AOP around the method (it will throw)")
        try {
            transactionPracticeService.saveTeamRuntimeException("aop-team")
        } catch (ex: Exception) {
            log.info("Runner: expected exception: ${ex::class.simpleName}")
        }
    }
}

