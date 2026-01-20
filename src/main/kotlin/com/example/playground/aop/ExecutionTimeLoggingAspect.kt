package com.example.playground.aop

import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Aspect
@Component
class ExecutionTimeLoggingAspect {
    private val log = LoggerFactory.getLogger(javaClass)

    @Around("execution(* com.example.playground.service..*(..))")
    fun logExecutionTime(joinPoint: ProceedingJoinPoint): Any? {
        val start = System.currentTimeMillis()
        try {
            val result = joinPoint.proceed()
            val timeTaken = System.currentTimeMillis() - start
            log.info("[AOP] ${joinPoint.signature.declaringTypeName}.${joinPoint.signature.name} executed in ${timeTaken}ms")
            return result
        } catch (ex: Throwable) {
            val timeTaken = System.currentTimeMillis() - start
            log.info("[AOP] ${joinPoint.signature.declaringTypeName}.${joinPoint.signature.name} threw ${ex::class.simpleName} after ${timeTaken}ms")
            throw ex
        }
    }
}

