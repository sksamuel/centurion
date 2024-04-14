package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class RecordEncoderTest : FunSpec({

   test("basic test") {
      data class Foo(val a: String, val b: Boolean)

      val schema = SchemaBuilder.record("Foo").fields().requiredString("a").requiredBoolean("b").endRecord()
      val actual = ReflectionRecordEncoder().encode(schema, Foo("hello", true))

      val expected = GenericData.Record(schema)
      expected.put("a", Utf8("hello"))
      expected.put("b", true)

      actual shouldBe expected
   }

   test("enums") {
      data class Foo(val wine: Wine)

      val wineSchema = Schema.createEnum("Wine", null, null, listOf("Shiraz", "Malbec"))
      val schema = SchemaBuilder.record("Foo").fields()
         .name("wine").type(wineSchema).noDefault()
         .endRecord()

      val actual = ReflectionRecordEncoder().encode(schema, Foo(Wine.Malbec))

      val expected = GenericData.Record(schema)
      expected.put("wine", GenericData.get().createEnum("Malbec", wineSchema))

      actual shouldBe expected
   }
})

enum class Wine { Shiraz, Malbec }
