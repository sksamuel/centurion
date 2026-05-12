package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema

class PrimitiveEncodersTest : FunSpec({

   test("ByteEncoder returns the input Byte") {
      ByteEncoder.encode(Schema.create(Schema.Type.INT), 7.toByte()) shouldBe 7.toByte()
   }

   test("ShortEncoder returns the input Short") {
      ShortEncoder.encode(Schema.create(Schema.Type.INT), 3.toShort()) shouldBe 3.toShort()
   }

   test("IntEncoder returns the input Int") {
      IntEncoder.encode(Schema.create(Schema.Type.INT), 42) shouldBe 42
   }

   test("LongEncoder returns the input Long") {
      LongEncoder.encode(Schema.create(Schema.Type.LONG), 9_999_999_999L) shouldBe 9_999_999_999L
   }

   test("BooleanEncoder returns the input Boolean") {
      BooleanEncoder.encode(Schema.create(Schema.Type.BOOLEAN), true) shouldBe true
      BooleanEncoder.encode(Schema.create(Schema.Type.BOOLEAN), false) shouldBe false
   }

   test("FloatEncoder returns the input Float") {
      FloatEncoder.encode(Schema.create(Schema.Type.FLOAT), 1.25f) shouldBe 1.25f
   }

   test("DoubleEncoder returns the input Double") {
      DoubleEncoder.encode(Schema.create(Schema.Type.DOUBLE), 3.14) shouldBe 3.14
   }
})
