package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema

class LongDecoderTest : FunSpec({

   beforeSpec {
      Decoder.useStrictPrimitiveDecoders = false
   }

   test("bytes") {
      val schema = Schema.create(Schema.Type.LONG)
      LongDecoder.decode(schema, 1.toByte()) shouldBe 1L
   }

   test("shorts") {
      val schema = Schema.create(Schema.Type.LONG)
      LongDecoder.decode(schema, 1.toShort()) shouldBe 1L
   }
   test("ints") {
      val schema = Schema.create(Schema.Type.LONG)
      LongDecoder.decode(schema, 1) shouldBe 1L
   }

   test("longs") {
      val schema = Schema.create(Schema.Type.LONG)
      LongDecoder.decode(schema, 1L) shouldBe 1L
   }

})
