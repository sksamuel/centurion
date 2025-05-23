package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeTypeOf
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

class StringEncoderTest : FunSpec({

   test("should use java string when String prop is set on record schema") {
      data class Foo(val a: String)

      val schema = SchemaBuilder.builder().record("foo").fields().name("a").type().stringType().noDefault().endRecord()
      GenericData.setStringType(schema, GenericData.StringType.String)
      val record = ReflectionRecordEncoder<Foo>(schema).encode(schema, Foo("boo")) as GenericRecord
      record["a"] shouldBe "boo"
   }

   test("should use Utf8 when prop is not set") {
      val schema = SchemaBuilder.builder().stringType()
      StringEncoder.encode(schema, "hello").shouldBeTypeOf<Utf8>()
   }

})
