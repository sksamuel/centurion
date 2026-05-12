package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.io.serde.DataSerde
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.file.CodecFactory
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

class DataSerdeTest : FunSpec({

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

   test("round trip happy path with no codec") {
      val serde = DataSerde<MyObj>(EncoderFactory.get(), DecoderFactory.get(), null)
      serde.deserialize(serde.serialize(profile)) shouldBe profile
   }

   test("round trip happy path with deflate codec") {
      val serde = DataSerde<MyObj>(EncoderFactory.get(), DecoderFactory.get(), CodecFactory.deflateCodec(6))
      serde.deserialize(serde.serialize(profile)) shouldBe profile
   }

   test("reuses ThreadLocal buffer across calls") {
      val serde = DataSerde<MyObj>(EncoderFactory.get(), DecoderFactory.get(), null)
      repeat(5) {
         serde.deserialize(serde.serialize(profile)) shouldBe profile
      }
   }
})
