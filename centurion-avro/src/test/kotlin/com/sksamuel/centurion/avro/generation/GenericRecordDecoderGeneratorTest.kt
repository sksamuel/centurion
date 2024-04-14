package com.sksamuel.centurion.avro.generation

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class GenericRecordDecoderGeneratorTest : FunSpec({

   test("simple decoder") {
      RecordDecoderGenerator().generate(
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

import com.sksamuel.centurion.avro.generation.GenericRecordDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

/**
 * This is a generated [GenericRecordDecoder] that decodes Avro [GenericRecord]s to [Foo]s
 */
object FooDecoder : GenericRecordDecoder<Foo> {
  override fun decode(record: GenericRecord): Foo {
    return Foo(
      a = record.get("a"),
      b = record.get("b"),
    )
  }
}
""".trim()
   }

})
