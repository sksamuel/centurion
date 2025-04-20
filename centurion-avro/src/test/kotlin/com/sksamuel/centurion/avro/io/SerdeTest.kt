package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.User
import com.sksamuel.centurion.avro.UserType
import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.io.serde.ReflectionSerdeFactory
import com.sksamuel.centurion.avro.io.serde.SerdeOptions
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlin.random.Random

class SerdeTest : FunSpec({

   beforeSpec {
      Decoder.useStrictPrimitiveDecoders = false
   }

   test("round trip happy path") {
      val user = User(
         Random.nextLong(),
         "sammy mcsamface",
         "sammy@mcsamface.com",
         Random.nextLong(),
         UserType.Admin,
         "Chicago",
         45,
         180,
         200
      )
      val serde = ReflectionSerdeFactory.create<User>(Format.Binary, SerdeOptions())
      serde.deserialize(serde.serialize(user)) shouldBe user
   }
})
