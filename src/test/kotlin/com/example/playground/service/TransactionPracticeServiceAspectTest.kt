package com.example.playground.service

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class TransactionPracticeServiceAspectTest(@Autowired private val service: TransactionPracticeService) {
    //test
    @Test
    fun `aop should intercept service methods`() {
        try {
            service.saveTeamRuntimeException("test-aop")
        } catch (_: Exception) {
            // expected
        }
    }
}
