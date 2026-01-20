package com.example.playground.domain

import jakarta.persistence.*

@Entity
class Team(
    @Column(nullable = false)
    var name: String
) {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long? = null

    @OneToMany(mappedBy = "team", cascade = [CascadeType.ALL], fetch = FetchType.LAZY)
    var members: MutableList<Member> = mutableListOf()
}