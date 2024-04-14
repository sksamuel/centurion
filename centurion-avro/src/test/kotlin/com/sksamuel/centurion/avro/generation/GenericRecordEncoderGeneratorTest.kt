package com.sksamuel.centurion.avro.generation

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class GenericRecordEncoderGeneratorTest : FunSpec({

   test("simple encoder") {
      GenericRecordEncoderGenerator().generate(
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

import com.sksamuel.centurion.avro.generation.GenericRecordEncoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

/**
 * This is a generated [GenericRecordEncoder] that encodes [Foo]s to Avro [GenericRecord]s
 */
object FooEncoder : GenericRecordEncoder<Foo> {
  private val schema = Schema.create(Schema.Type.STRING)
  override fun encode(value: Foo): GenericRecord {
    val record = GenericData.Record(schema)
    record.put("a", value.a)
    record.put("b", value.b)
    return record
  }
}
""".trim()
   }

})
