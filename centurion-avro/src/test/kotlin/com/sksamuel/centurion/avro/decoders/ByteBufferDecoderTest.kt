package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

class ByteBufferDecoderTest : FunSpec({

   test("decode from ByteArray wraps it without copy") {
      val schema = Schema.create(Schema.Type.BYTES)
      val bytes = byteArrayOf(1, 2, 3)
      val decoded = ByteBufferDecoder.decode(schema, bytes)
      val out = ByteArray(decoded.remaining())
      decoded.get(out)
      out shouldBe bytes
   }

   test("decode from GenericFixed wraps its bytes") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val fixed = GenericData.get().createFixed(null, byteArrayOf(7, 8, 9), schema) as GenericData.Fixed
      val decoded = ByteBufferDecoder.decode(schema, fixed)
      val out = ByteArray(decoded.remaining())
      decoded.get(out)
      out shouldBe byteArrayOf(7, 8, 9)
   }

   test("decode from ByteBuffer reads only the remaining bytes") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4, 5))
      buffer.position(2)
      val decoded = ByteBufferDecoder.decode(schema, buffer)
      val out = ByteArray(decoded.remaining())
      decoded.get(out)
      out shouldBe byteArrayOf(3, 4, 5)
   }

   test("decode does not mutate the source ByteBuffer position") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4))
      buffer.position(1)
      ByteBufferDecoder.decode(schema, buffer)
      buffer.position() shouldBe 1
      buffer.remaining() shouldBe 3
   }

   test("decoding the same ByteBuffer twice yields the same bytes") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4))
      buffer.position(1)
      val out1 = ByteArray(3).also { ByteBufferDecoder.decode(schema, buffer).get(it) }
      val out2 = ByteArray(3).also { ByteBufferDecoder.decode(schema, buffer).get(it) }
      out1 shouldBe byteArrayOf(2, 3, 4)
      out2 shouldBe byteArrayOf(2, 3, 4)
   }
})
