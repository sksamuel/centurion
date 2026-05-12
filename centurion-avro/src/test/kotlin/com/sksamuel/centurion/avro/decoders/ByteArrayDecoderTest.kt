package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

class ByteArrayDecoderTest : FunSpec({

   test("decode from ByteArray returns the input as-is") {
      val schema = Schema.create(Schema.Type.BYTES)
      val bytes = byteArrayOf(1, 2, 3)
      ByteArrayDecoder.decode(schema, bytes) shouldBe bytes
   }

   test("decode from GenericFixed returns its bytes") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val fixed = GenericData.get().createFixed(null, byteArrayOf(7, 8, 9), schema) as GenericData.Fixed
      ByteArrayDecoder.decode(schema, fixed) shouldBe byteArrayOf(7, 8, 9)
   }

   test("decode from ByteBuffer reads only the remaining bytes") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4, 5))
      buffer.position(2)
      ByteArrayDecoder.decode(schema, buffer) shouldBe byteArrayOf(3, 4, 5)
   }

   test("decode does not mutate the source ByteBuffer position") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4))
      buffer.position(1)
      ByteArrayDecoder.decode(schema, buffer)
      buffer.position() shouldBe 1
      buffer.remaining() shouldBe 3
   }

   test("decoding the same ByteBuffer twice yields the same bytes") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4))
      buffer.position(1)
      ByteArrayDecoder.decode(schema, buffer) shouldBe byteArrayOf(2, 3, 4)
      ByteArrayDecoder.decode(schema, buffer) shouldBe byteArrayOf(2, 3, 4)
   }
})
