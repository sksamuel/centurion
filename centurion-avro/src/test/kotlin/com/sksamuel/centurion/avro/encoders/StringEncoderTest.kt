package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.types.shouldBeTypeOf
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class StringEncoderTest : FunSpec({

   test("should use java string when prop is set") {
      val schema = SchemaBuilder.builder().stringType()
      GenericData.setStringType(schema, GenericData.StringType.String)
      StringEncoder.encode(schema, "hello").shouldBeTypeOf<String>()
   }

   test("should use Utf8 when prop is not set") {
      val schema = SchemaBuilder.builder().stringType()
      StringEncoder.encode(schema, "hello").shouldBeTypeOf<Utf8>()
   }

})
