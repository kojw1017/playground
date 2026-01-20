package com.example.playground.domain

import jakarta.persistence.*

@Entity
class Member(
    @Column(nullable = false)
    var username: String,

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "team_id")
    var team: Team
) {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null
}