package com.sksamuel.centurion.avro

import kotlin.random.Random

data class User(
  val userId: Long,
  val name: String?,
  val email: String?,
  val lastActiveTimestamp: Long,
  val type: UserType?,
  val location: String,
  val age: Int,
  val height: Int,
  val weight: Int,
)

enum class UserType { User, Admin }

val user = User(
  userId = Random.nextLong(),
  name = "sammy mcsamface",
  email = "sammy@mcsamface.com",
  lastActiveTimestamp = Random.nextLong(),
  type = UserType.Admin,
  location = "Chicago",
  age = 45,
  height = 180,
  weight = 200,
)
