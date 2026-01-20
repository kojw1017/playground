package com.example.playground.service

import com.example.playground.domain.Team
import com.example.playground.repository.TeamRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.support.TransactionSynchronizationManager

@Service
class TransactionPracticeService(private val teamRepository: TeamRepository) {
    private val log = LoggerFactory.getLogger(javaClass)

    // Scenario 1: Unchecked Exception (RuntimeException) -> Default Rollback
    @Transactional
    fun saveTeamRuntimeException(name: String) {
        teamRepository.save(Team(name = name))
        log.info("Team saved. Throwing RuntimeException...")
        throw RuntimeException("Intended RuntimeException for Rollback Test")
    }

    // Scenario 2: Checked Exception (Exception) -> Commit (Default behavior)
    @Transactional
    fun saveTeamCheckedException(name: String) {
        teamRepository.save(Team(name = name))
        log.info("Team saved. Throwing Checked Exception...")
        throw Exception("Intended Checked Exception for Commit Test")
    }

    // Scenario 3: Self-Invocation -> Transaction ignored
    // This method has NO @Transactional
    fun externalCall(name: String) {
        log.info(
                "External call started. Active Transaction: ${TransactionSynchronizationManager.isActualTransactionActive()}"
        )
        internalMarkTransactional(name)
    }

    // This method HAS @Transactional, but calling it from within the SAME class (via externalCall)
    // bypasses the proxy.
    @Transactional
    fun internalMarkTransactional(name: String) {
        log.info(
                "Internal call started. Active Transaction: ${TransactionSynchronizationManager.isActualTransactionActive()}"
        )
        teamRepository.save(Team(name = name))
        // If transaction was active, this RuntimeException should rollback the save.
        // If NOT active (self-invocation), the save might persist depending on auto-commit,
        // but here we are testing IF the transaction context is active.
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            log.warn("Warning: Transaction is NOT active! Self-invocation bypassed the proxy.")
        }
    }

    // Scenario 4: ReadOnly = true -> Dirty Checking might be skipped (depending on provider),
    // but mainly it's a performance optimization.
    // In some setups, it also prevents writing to DB (if routed to slave or set on connection).
    @Transactional(readOnly = true)
    fun updateTeamReadOnly(name: String, newName: String) {
        val team = teamRepository.save(Team(name = name))
        // Note: save() itself might flush immediately if using IDENTITY generation with some
        // providers,
        // but here we want to test if modifying the entity AFTER retrieval (or save) triggers an
        // update.
        // Actually, let's look up an existing team and try to modify it.
    }

    @Transactional(readOnly = true)
    fun findByAndModify(id: Long, newName: String) {
        val team = teamRepository.findById(id).orElseThrow()
        team.name = newName
        // If readOnly=true, Hibernate usually sets flush mode to MANUAL, so dirty checking won't
        // trigger an update automatically at commit.
    }
}
