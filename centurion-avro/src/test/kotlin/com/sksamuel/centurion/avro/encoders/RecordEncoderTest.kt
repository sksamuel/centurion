package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class RecordEncoderTest : FunSpec({

   test("basic test") {
      data class Foo(val a: String, val b: Boolean)

      val schema = SchemaBuilder.record("Foo").fields().requiredString("a").requiredBoolean("b").endRecord()
      val actual = RecordEncoder().encode(schema, Foo("hello", true))

      val expected = GenericData.Record(schema)
      expected.put("a", Utf8("hello"))
      expected.put("b", true)

      actual shouldBe expected
   }

})
