package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class RecordDecoderTest : FunSpec({

   test("basic record decoder") {
      data class Foo(val a: String, val b: Boolean)
      val schema = SchemaBuilder.record("Foo").fields().requiredString("a").requiredBoolean("b").endRecord()

      val record = GenericData.Record(schema)
      record.put("a", Utf8("hello"))
      record.put("b", true)

      ReflectionRecordDecoder<Foo>().decode(schema, record) shouldBe Foo("hello", true)
   }

})
