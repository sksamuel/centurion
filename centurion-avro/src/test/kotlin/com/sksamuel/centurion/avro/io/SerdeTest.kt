package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.encoders.User
import com.sksamuel.centurion.avro.encoders.UserType
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.ints.shouldBeLessThan
import io.kotest.matchers.shouldBe
import org.apache.avro.file.BZip2Codec
import kotlin.random.Random

class SerdeTest : FunSpec({

   test("round trip happy path") {
      val user = User(Random.nextLong(), "sammy mcsamface", "sammy@mcsamface.com", Random.nextLong(), UserType.Admin)
      val serde = Serde<User>()
      serde.deserialize(serde.serialize(user)) shouldBe user
   }

   test("with compression") {
      val user = User(
         Random.nextLong(),
         "sammy mcsamface long text here so we have something to compress long text here so we have something to compress",
         "sammy@mcsamface.com long text here so we have something to compress long text here so we have something to compress",
         Random.nextLong(),
         UserType.Admin
      )
      val serde1 = Serde<User>()
      val serde2 = Serde<User>(SerdeOptions(codec = BZip2Codec()))
      serde1.serialize(user).size shouldBeLessThan serde2.serialize(user).size
      serde2.deserialize(serde2.serialize(user)) shouldBe user
   }
})
