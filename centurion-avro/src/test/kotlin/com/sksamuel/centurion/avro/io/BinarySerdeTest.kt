package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.io.serde.BinarySerde
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

class BinarySerdeTest : FunSpec({

   test("round trip happy path") {
      val profile = MyObj(
         long = 123,
         nullableLong = 0,
         nullableString = null,
         age = 432,
         double = null,
         bool = true,
         strings = setOf("foo", "bar"),
         ints = setOf(1, 2),
         nullableMap = null,
      )
      val serde = BinarySerde<MyObj>(EncoderFactory.get(), DecoderFactory.get())
      serde.deserialize(serde.serialize(profile)) shouldBe profile
   }
})

data class MyObj(
   val long: Long,
   val nullableLong: Long?,
   val nullableString: String?,
   val age: Int?,
   val double: Double?,
   val bool: Boolean,
   val strings: Set<String>,
   val ints: Set<Int>,
   val nullableMap: Map<String, Map<String, String>>?,
)
