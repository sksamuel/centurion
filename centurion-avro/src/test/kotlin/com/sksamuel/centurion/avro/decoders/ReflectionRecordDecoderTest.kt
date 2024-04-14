package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.encoders.Wine
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class ReflectionRecordDecoderTest : FunSpec({

   test("basic record decoder") {
      data class Foo(val a: String, val b: Boolean)

      val schema = SchemaBuilder.record("Foo").fields().requiredString("a").requiredBoolean("b").endRecord()

      val record = GenericData.Record(schema)
      record.put("a", Utf8("hello"))
      record.put("b", true)

      ReflectionRecordDecoder<Foo>().decode(schema, record) shouldBe Foo("hello", true)
   }

   test("enums") {
      data class Foo(val wine: Wine)

      val wineSchema = Schema.createEnum("Wine", null, null, listOf("Shiraz", "Malbec"))
      val schema = SchemaBuilder.record("Foo").fields()
         .name("wine").type(wineSchema).noDefault()
         .endRecord()

      val record = GenericData.Record(schema)
      record.put("wine", GenericData.get().createEnum("Shiraz", wineSchema))

      ReflectionRecordDecoder<Foo>().decode(schema, record) shouldBe Foo(Wine.Shiraz)
   }

   test("nulls") {
      data class Foo(val a: String?, val b: String)
      val schema = SchemaBuilder.record("Foo").fields()
         .optionalString("a")
         .requiredString("b")
         .endRecord()

      val record = GenericData.Record(schema)
      record.put("a", null)
      record.put("b", Utf8("hello"))

      ReflectionRecordDecoder<Foo>().decode(schema, record) shouldBe Foo(null, "hello")
   }
})
