package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.User
import com.sksamuel.centurion.avro.io.serde.CachedSerdeFactory
import com.sksamuel.centurion.avro.io.serde.ReflectionSerdeFactory
import com.sksamuel.centurion.avro.io.serde.SerdeOptions
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.types.shouldBeSameInstanceAs

class CachedReflectionSerdeFactoryTest : FunSpec({

   test("should cache instances") {

      val factory = CachedSerdeFactory(ReflectionSerdeFactory)

      val serde1 = factory.create<User>(Format.Binary, SerdeOptions())
      val serde2 = factory.create<User>(Format.Binary, SerdeOptions())

      serde1.shouldBeSameInstanceAs(serde2)
   }

})
