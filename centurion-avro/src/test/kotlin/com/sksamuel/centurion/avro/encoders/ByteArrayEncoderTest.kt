package com.sksamuel.centurion.avro.encoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

class ByteArrayEncoderTest : FunSpec({

   test("BYTES: returns the input ByteArray") {
      val schema = Schema.create(Schema.Type.BYTES)
      val bytes = byteArrayOf(1, 2, 3)
      ByteArrayEncoder.encode(schema, bytes) shouldBe bytes
   }

   test("FIXED: routes through FixedByteArrayEncoder") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val encoded = ByteArrayEncoder.encode(schema, byteArrayOf(7, 8, 9)) as GenericData.Fixed
      encoded.bytes() shouldBe byteArrayOf(7, 8, 9)
   }

   test("ByteArrayEncoder rejects unsupported schema type") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalStateException> {
         ByteArrayEncoder.encode(schema, byteArrayOf(1))
      }
   }

   test("FixedByteArrayEncoder encodes an exact-size array") {
      val schema = SchemaBuilder.builder().fixed("f").size(4)
      val encoded = FixedByteArrayEncoder.encode(schema, byteArrayOf(1, 2, 3, 4)) as GenericData.Fixed
      encoded.bytes() shouldBe byteArrayOf(1, 2, 3, 4)
   }

   test("FixedByteArrayEncoder zero-pads shorter input") {
      val schema = SchemaBuilder.builder().fixed("f").size(5)
      val encoded = FixedByteArrayEncoder.encode(schema, byteArrayOf(1, 2)) as GenericData.Fixed
      encoded.bytes() shouldBe byteArrayOf(1, 2, 0, 0, 0)
   }

   test("FixedByteArrayEncoder rejects oversize input with a useful error") {
      val schema = SchemaBuilder.builder().fixed("f").size(2)
      val ex = shouldThrow<IllegalStateException> {
         FixedByteArrayEncoder.encode(schema, byteArrayOf(1, 2, 3))
      }
      ex.message!! shouldContain "3"
      ex.message!! shouldContain "2"
   }

   test("FixedByteArrayEncoder requires a FIXED schema") {
      val schema = Schema.create(Schema.Type.BYTES)
      shouldThrow<IllegalArgumentException> {
         FixedByteArrayEncoder.encode(schema, byteArrayOf(1))
      }
   }
})
