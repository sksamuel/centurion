package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.decoders.ByteBufferDecoder
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

class ByteBufferEncoderTest : FunSpec({

   test("BYTES: encode a freshly-wrapped ByteBuffer") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4))
      ByteBufferEncoder.encode(schema, buffer) shouldBe byteArrayOf(1, 2, 3, 4)
   }

   test("BYTES: honour a positioned ByteBuffer (no leakage of bytes before position)") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4, 5))
      buffer.position(2)
      ByteBufferEncoder.encode(schema, buffer) shouldBe byteArrayOf(3, 4, 5)
   }

   test("BYTES: honour a positioned-and-limited ByteBuffer") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4, 5))
      buffer.position(1)
      buffer.limit(4)
      ByteBufferEncoder.encode(schema, buffer) shouldBe byteArrayOf(2, 3, 4)
   }

   test("BYTES: honour a sliced ByteBuffer (non-zero arrayOffset)") {
      val schema = Schema.create(Schema.Type.BYTES)
      val backing = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4, 5))
      backing.position(2)
      val sliced = backing.slice()
      ByteBufferEncoder.encode(schema, sliced) shouldBe byteArrayOf(3, 4, 5)
   }

   test("BYTES: encoder must not mutate the source buffer's position") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4))
      buffer.position(1)
      ByteBufferEncoder.encode(schema, buffer)
      buffer.position() shouldBe 1
      buffer.remaining() shouldBe 3
   }

   test("BYTES round-trip through ByteBufferDecoder for a positioned buffer") {
      val schema = Schema.create(Schema.Type.BYTES)
      val source = ByteBuffer.wrap(byteArrayOf(10, 20, 30, 40, 50))
      source.position(2)
      val encoded = ByteBufferEncoder.encode(schema, source)
      val decoded = ByteBufferDecoder.decode(schema, encoded)
      val out = ByteArray(decoded.remaining())
      decoded.get(out)
      out shouldBe byteArrayOf(30, 40, 50)
   }

   test("FIXED via ByteBufferEncoder: encode an exact-size buffer") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val buffer = ByteBuffer.wrap(byteArrayOf(7, 8, 9))
      val encoded = ByteBufferEncoder.encode(schema, buffer) as GenericData.Fixed
      encoded.bytes() shouldBe byteArrayOf(7, 8, 9)
   }

   test("FIXED: a positioned ByteBuffer writes bytes from the position, not the backing array start") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4, 5))
      buffer.position(2)
      val encoded = FixedByteBufferEncoder.encode(schema, buffer) as GenericData.Fixed
      encoded.bytes() shouldBe byteArrayOf(3, 4, 5)
   }

   test("FIXED: a buffer with fewer remaining bytes than the fixed size is zero-padded") {
      val schema = SchemaBuilder.builder().fixed("f").size(4)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2))
      val encoded = FixedByteBufferEncoder.encode(schema, buffer) as GenericData.Fixed
      encoded.bytes() shouldBe byteArrayOf(1, 2, 0, 0)
   }

   test("FIXED: encoder must not mutate the source buffer's position") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4))
      buffer.position(1)
      FixedByteBufferEncoder.encode(schema, buffer)
      buffer.position() shouldBe 1
      buffer.remaining() shouldBe 3
   }

   test("FIXED: raise an explicit error when the buffer is larger than the fixed size") {
      val schema = SchemaBuilder.builder().fixed("f").size(2)
      val buffer = ByteBuffer.wrap(byteArrayOf(1, 2, 3, 4))
      val ex = shouldThrow<IllegalStateException> {
         FixedByteBufferEncoder.encode(schema, buffer)
      }
      ex.message!! shouldContain "4"
      ex.message!! shouldContain "2"
   }

   test("FIXED: oversized backing array but in-bounds remaining() does not crash") {
      val schema = SchemaBuilder.builder().fixed("f").size(2)
      // Backing array is bigger than fixedSize, but the window we care about is 2 bytes.
      val buffer = ByteBuffer.wrap(byteArrayOf(9, 9, 1, 2, 9, 9))
      buffer.position(2)
      buffer.limit(4)
      val encoded = FixedByteBufferEncoder.encode(schema, buffer) as GenericData.Fixed
      encoded.bytes() shouldBe byteArrayOf(1, 2)
   }
})
