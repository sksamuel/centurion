package com.sksamuel.centurion.avro.decoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class StringVariantDecodersTest : FunSpec({

   test("ByteStringDecoder decodes a ByteArray") {
      val schema = Schema.create(Schema.Type.BYTES)
      ByteStringDecoder.decode(schema, "abc".encodeToByteArray()) shouldBe "abc"
   }

   test("ByteStringDecoder decodes a ByteBuffer") {
      val schema = Schema.create(Schema.Type.BYTES)
      ByteStringDecoder.decode(schema, ByteBuffer.wrap("abc".encodeToByteArray())) shouldBe "abc"
   }

   test("ByteStringDecoder requires a BYTES schema") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalArgumentException> {
         ByteStringDecoder.decode(schema, "x".encodeToByteArray())
      }
   }

   test("ByteStringDecoder rejects a non-bytes value") {
      val schema = Schema.create(Schema.Type.BYTES)
      shouldThrow<IllegalStateException> {
         ByteStringDecoder.decode(schema, "not bytes")
      }
   }

   test("GenericFixedStringDecoder decodes a GenericFixed") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val fixed = GenericData.get().createFixed(null, "abc".encodeToByteArray(), schema) as GenericData.Fixed
      GenericFixedStringDecoder.decode(schema, fixed) shouldBe "abc"
   }

   test("GenericFixedStringDecoder requires a FIXED schema") {
      val schema = Schema.create(Schema.Type.BYTES)
      shouldThrow<IllegalArgumentException> {
         GenericFixedStringDecoder.decode(schema, byteArrayOf(1))
      }
   }

   test("GenericFixedStringDecoder rejects a non-GenericFixed value") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      shouldThrow<IllegalStateException> {
         GenericFixedStringDecoder.decode(schema, byteArrayOf(1, 2, 3))
      }
   }

   test("UTF8Decoder decodes a String") {
      val schema = Schema.create(Schema.Type.STRING)
      UTF8Decoder.decode(schema, "hello") shouldBe Utf8("hello")
   }

   test("UTF8Decoder decodes a ByteArray") {
      val schema = Schema.create(Schema.Type.BYTES)
      UTF8Decoder.decode(schema, "world".encodeToByteArray()) shouldBe Utf8("world")
   }

   test("UTF8Decoder decodes a ByteBuffer") {
      val schema = Schema.create(Schema.Type.BYTES)
      UTF8Decoder.decode(schema, ByteBuffer.wrap("hi".encodeToByteArray())) shouldBe Utf8("hi")
   }

   test("UTF8Decoder decodes a GenericFixed") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val fixed = GenericData.get().createFixed(null, "foo".encodeToByteArray(), schema) as GenericData.Fixed
      UTF8Decoder.decode(schema, fixed) shouldBe Utf8("foo")
   }

   test("UTF8Decoder rejects unsupported values") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalStateException> {
         UTF8Decoder.decode(schema, 12345)
      }
   }
})
