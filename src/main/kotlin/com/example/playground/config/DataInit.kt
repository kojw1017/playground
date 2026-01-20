package com.example.playground.config

import com.example.playground.domain.Member
import com.example.playground.domain.Team
import com.example.playground.repository.TeamRepository
import jakarta.annotation.PostConstruct
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class DataInit(
    private val teamRepository: TeamRepository
) {
    @PostConstruct
    @Transactional
    fun init() {
        // 데이터가 조금이라도 있으면 싹 지우고 다시 시작 (리셋 로직 추가)
        if (teamRepository.count() > 0) {
            return
        }

        val teams = mutableListOf<Team>()

        // 팀 2,000개 생성 (쿼리 2,001번 나갈 예정)
        for (i in 1..2000) {
            val team = Team(name = "팀-$i")
            // 팀당 멤버 2명만 (너무 많으면 메모리 터질 수 있으니 조절)
            for (j in 1..2) {
                team.members.add(Member(username = "멤버-$i-$j", team = team))
            }
            teams.add(team)
        }

        // saveAll로 한 번에 저장 (Insert 성능 최적화가 아니라 N+1 테스트용 데이터니까)
        teamRepository.saveAll(teams)
    }
}