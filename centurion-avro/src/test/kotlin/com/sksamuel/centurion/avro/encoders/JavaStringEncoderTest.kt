package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

class JavaStringEncoderTest : FunSpec({

   test("JavaStringEncoder returns the same Java String regardless of schema") {
      val input = "hello"
      val encoded = JavaStringEncoder.encode(Schema.create(Schema.Type.STRING), input)
      encoded.shouldBeInstanceOf<String>()
      encoded shouldBeSameInstanceAs input
   }

   test("JavaStringEncoder ignores the schema's STRING_PROP setting") {
      val schema = SchemaBuilder.builder().stringType()
      GenericData.setStringType(schema, GenericData.StringType.CharSequence)
      JavaStringEncoder.encode(schema, "world") shouldBe "world"
   }
})
