package com.sksamuel.centurion.avro.decoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.avro.Schema

class PrimitiveDecodersTest : FunSpec({

   test("ByteDecoder accepts Byte") {
      ByteDecoder.decode(Schema.create(Schema.Type.INT), 7.toByte()) shouldBe 7.toByte()
   }

   test("ByteDecoder rejects Int with a useful message") {
      val ex = shouldThrow<IllegalStateException> {
         ByteDecoder.decode(Schema.create(Schema.Type.INT), 1)
      }
      ex.message!! shouldContain "ByteDecoder"
   }

   test("ShortDecoder widens Byte to Short") {
      ShortDecoder.decode(Schema.create(Schema.Type.INT), 3.toByte()) shouldBe 3.toShort()
      ShortDecoder.decode(Schema.create(Schema.Type.INT), 3.toShort()) shouldBe 3.toShort()
   }

   test("IntDecoder widens Byte and Short to Int") {
      IntDecoder.decode(Schema.create(Schema.Type.INT), 4.toByte()) shouldBe 4
      IntDecoder.decode(Schema.create(Schema.Type.INT), 4.toShort()) shouldBe 4
      IntDecoder.decode(Schema.create(Schema.Type.INT), 4) shouldBe 4
   }

   test("DoubleDecoder widens Float to Double") {
      DoubleDecoder.decode(Schema.create(Schema.Type.DOUBLE), 1.5f) shouldBe 1.5
      DoubleDecoder.decode(Schema.create(Schema.Type.DOUBLE), 1.5) shouldBe 1.5
   }

   test("FloatDecoder accepts Float only") {
      FloatDecoder.decode(Schema.create(Schema.Type.FLOAT), 2.5f) shouldBe 2.5f
      shouldThrow<IllegalStateException> {
         FloatDecoder.decode(Schema.create(Schema.Type.FLOAT), 2.5)
      }
   }

   test("BooleanDecoder accepts Boolean") {
      BooleanDecoder.decode(Schema.create(Schema.Type.BOOLEAN), true) shouldBe true
      BooleanDecoder.decode(Schema.create(Schema.Type.BOOLEAN), false) shouldBe false
      val ex = shouldThrow<IllegalStateException> {
         BooleanDecoder.decode(Schema.create(Schema.Type.BOOLEAN), "true")
      }
      ex.message!! shouldContain "BooleanDecoder"
   }

   test("error messages identify the decoder type") {
      shouldThrow<IllegalStateException> {
         IntDecoder.decode(Schema.create(Schema.Type.INT), "x")
      }.message!! shouldContain "IntDecoder"
      shouldThrow<IllegalStateException> {
         LongDecoder.decode(Schema.create(Schema.Type.LONG), "x")
      }.message!! shouldContain "LongDecoder"
      shouldThrow<IllegalStateException> {
         DoubleDecoder.decode(Schema.create(Schema.Type.DOUBLE), "x")
      }.message!! shouldContain "DoubleDecoder"
   }
})
