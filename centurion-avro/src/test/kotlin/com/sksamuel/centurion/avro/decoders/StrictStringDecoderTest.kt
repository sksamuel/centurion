package com.sksamuel.centurion.avro.decoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class StrictStringDecoderTest : FunSpec({

   test("STRING schema accepts String") {
      val schema = Schema.create(Schema.Type.STRING)
      StrictStringDecoder.decode(schema, "hello") shouldBe "hello"
   }

   test("STRING schema accepts Utf8") {
      val schema = Schema.create(Schema.Type.STRING)
      StrictStringDecoder.decode(schema, Utf8("world")) shouldBe "world"
   }

   test("STRING schema rejects a ByteArray (unlike StringDecoder)") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalStateException> {
         StrictStringDecoder.decode(schema, "abc".encodeToByteArray())
      }
   }

   test("BYTES schema delegates to ByteStringDecoder") {
      val schema = Schema.create(Schema.Type.BYTES)
      StrictStringDecoder.decode(schema, ByteBuffer.wrap("xyz".encodeToByteArray())) shouldBe "xyz"
   }

   test("FIXED schema delegates to GenericFixedStringDecoder") {
      val schema = SchemaBuilder.builder().fixed("f").size(3)
      val fixed = GenericData.get().createFixed(null, "foo".encodeToByteArray(), schema) as GenericData.Fixed
      StrictStringDecoder.decode(schema, fixed) shouldBe "foo"
   }

   test("rejects other schema types") {
      val schema = Schema.create(Schema.Type.INT)
      shouldThrow<IllegalStateException> {
         StrictStringDecoder.decode(schema, 1)
      }
   }
})
