package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import java.nio.ByteBuffer

class StringDecoderBufferPositionTest : FunSpec({

   test("StringDecoder reads only remaining bytes from a positioned ByteBuffer") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap("xxhello".encodeToByteArray())
      buffer.position(2)
      StringDecoder.decode(schema, buffer) shouldBe "hello"
   }

   test("StringDecoder does not mutate the source buffer position") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap("xxhello".encodeToByteArray())
      buffer.position(2)
      StringDecoder.decode(schema, buffer)
      buffer.position() shouldBe 2
      buffer.remaining() shouldBe 5
   }

   test("UTF8Decoder does not mutate the source buffer position") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap("..world".encodeToByteArray())
      buffer.position(2)
      UTF8Decoder.decode(schema, buffer).toString() shouldBe "world"
      buffer.position() shouldBe 2
   }

   test("ByteStringDecoder does not mutate the source buffer position") {
      val schema = Schema.create(Schema.Type.BYTES)
      val buffer = ByteBuffer.wrap("__foo".encodeToByteArray())
      buffer.position(2)
      ByteStringDecoder.decode(schema, buffer) shouldBe "foo"
      buffer.position() shouldBe 2
   }
})
