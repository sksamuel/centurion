package com.sksamuel.centurion.avro.encoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

class FixedStringEncoderTest : FunSpec({

   test("encodes an exact-size string") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val encoded = FixedStringEncoder.encode(schema, "abc") as GenericData.Fixed
      encoded.bytes() shouldBe "abc".encodeToByteArray()
   }

   test("rejects an oversize string with a useful error") {
      val schema = SchemaBuilder.builder().fixed("f").size(2)
      val ex = shouldThrow<IllegalStateException> {
         FixedStringEncoder.encode(schema, "abcd")
      }
      ex.message!! shouldContain "4"
      ex.message!! shouldContain "2"
   }

   test("encodes a multi-byte UTF-8 string when its byte length equals fixedSize") {
      val schema = SchemaBuilder.builder().fixed("f").size(2)
      val encoded = FixedStringEncoder.encode(schema, "é") as GenericData.Fixed
      // "é" is 2 bytes in UTF-8
      encoded.bytes() shouldBe byteArrayOf(-61, -87)
   }
})
