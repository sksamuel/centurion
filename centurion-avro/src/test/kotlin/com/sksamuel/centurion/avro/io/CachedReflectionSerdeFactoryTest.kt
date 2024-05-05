package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.encoders.User
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.types.shouldBeSameInstanceAs

class CachedReflectionSerdeFactoryTest : FunSpec({

   test("should cache instances") {
      val serde1 = CachedReflectionSerdeFactory.create<User>()
      val serde2 = CachedReflectionSerdeFactory.create<User>()
      serde1.shouldBeSameInstanceAs(serde2)
   }

})
