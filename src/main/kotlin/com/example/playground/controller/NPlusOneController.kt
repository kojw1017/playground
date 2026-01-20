package com.example.playground.controller

import com.example.playground.repository.TeamRepository
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.transaction.annotation.Transactional

@RestController
class NPlusOneController(
    private val teamRepository: TeamRepository
) {

    // 1. 나쁜 예제 (N+1 발생)
    @GetMapping("/teams/bad")
    @Transactional(readOnly = true)
    fun findAllBad(): List<String> {
        val teams = teamRepository.findAll() // 쿼리 1번 (팀 조회)

        // 여기서 지옥 시작 (팀 개수만큼 쿼리 추가 발생)
        return teams.map { team ->
            "팀명: ${team.name}, 멤버수: ${team.members.size}명" // members 접근 시 쿼리 나감
        }
    }

    // 2. 좋은 예제 (Fetch Join)
    @GetMapping("/teams/good")
    @Transactional(readOnly = true)
    fun findAllGood(): List<String> {
        val teams = teamRepository.findAllWithFetchJoin() // 쿼리 1방

        return teams.map { team ->
            "팀명: ${team.name}, 멤버수: ${team.members.size}명"
        }
    }
}