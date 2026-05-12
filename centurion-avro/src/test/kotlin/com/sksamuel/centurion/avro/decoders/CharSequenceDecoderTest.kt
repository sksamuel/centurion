package com.sksamuel.centurion.avro.decoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class CharSequenceDecoderTest : FunSpec({

   test("CharSequenceDecoder decodes a Kotlin String") {
      CharSequenceDecoder.decode(Schema.create(Schema.Type.STRING), "hello") shouldBe "hello"
   }

   test("CharSequenceDecoder decodes a Utf8") {
      CharSequenceDecoder.decode(Schema.create(Schema.Type.STRING), Utf8("world")) shouldBe "world"
   }

   test("CharSequenceDecoder decodes a ByteArray") {
      CharSequenceDecoder.decode(Schema.create(Schema.Type.BYTES), "abc".encodeToByteArray()) shouldBe "abc"
   }

   test("CharSequenceDecoder decodes a ByteBuffer") {
      CharSequenceDecoder.decode(Schema.create(Schema.Type.BYTES), ByteBuffer.wrap("def".encodeToByteArray())) shouldBe "def"
   }

   test("CharSequenceDecoder decodes a GenericFixed") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val fixed = GenericData.get().createFixed(null, "ghi".encodeToByteArray(), schema) as GenericData.Fixed
      CharSequenceDecoder.decode(schema, fixed) shouldBe "ghi"
   }

   test("CharSequenceDecoder rejects an unsupported value") {
      shouldThrow<IllegalStateException> {
         CharSequenceDecoder.decode(Schema.create(Schema.Type.STRING), 12345)
      }
   }
})
