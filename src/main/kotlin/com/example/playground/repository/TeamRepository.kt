package com.example.playground.repository

import com.example.playground.domain.Team
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query

interface TeamRepository : JpaRepository<Team, Long> {
    // [해결책] 한방 쿼리 (Fetch Join)
    @Query("SELECT t FROM Team t JOIN FETCH t.members")
    fun findAllWithFetchJoin(): List<Team>
}