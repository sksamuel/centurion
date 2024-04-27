package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class CachedSpecificRecordEncoderTest : FunSpec({

   test("basic test") {
      data class Foo(val a: String, val b: Boolean)

      val schema = SchemaBuilder.record("Foo").fields().requiredString("a").requiredBoolean("b").endRecord()

      val expected = GenericData.Record(schema)
      expected.put("a", Utf8("hello"))
      expected.put("b", true)

      repeat(10) {
         val actual = CachedSpecificRecordEncoder().encode(schema, Foo("hello", true))
         actual shouldBe expected
      }
   }
})
