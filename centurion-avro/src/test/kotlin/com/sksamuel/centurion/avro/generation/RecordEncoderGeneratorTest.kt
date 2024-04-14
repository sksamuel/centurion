package com.sksamuel.centurion.avro.generation


import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class RecordEncoderGeneratorTest : FunSpec({

   test("simple encoder") {
      RecordEncoderGenerator().generate(
         DataClass(
            "a.b",
            "Foo",
            listOf(
               Member("a", Type.BooleanType),
               Member("b", Type.StringType),
            )
         )
      ).trim() shouldBe """
package a.b

import com.sksamuel.centurion.avro.encoders.*
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

/**
 * This is a generated [Encoder] that encodes [Foo]s to Avro [GenericRecord]s
 */
object FooEncoder : Encoder<Foo> {
  override fun encode(schema: Schema, value: Foo): GenericRecord {
    val record = GenericData.Record(schema)
    record.put("a", BooleanEncoder.encode(schema.getField("a").schema(), value.a))
    record.put("b", StringEncoder.encode(schema.getField("b").schema(), value.b))
    return record
  }
}
""".trim()
   }

})
