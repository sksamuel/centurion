package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.encoders.User
import com.sksamuel.centurion.avro.encoders.UserType
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlin.random.Random

class SerdeTest : FunSpec({

   test("round trip happy path") {
      val user = User(Random.nextLong(), "sammy mcsamface", "sammy@mcsamface.com", Random.nextLong(), UserType.Admin)
      val serde = ReflectionSerdeFactory.create<User>()
      serde.deserialize(serde.serialize(user)) shouldBe user
   }
})
