package com.sksamuel.centurion.avro.generation

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class RecordDecoderGeneratorTest : FunSpec({

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

import com.sksamuel.centurion.avro.decoders.*
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

/**
 * This is a generated [Decoder] that deserializes Avro [GenericRecord]s to [Foo]s
 */
object FooDecoder : Decoder<Foo> {
  override fun decode(schema: Schema, value: Any?): Foo {
    require(value is GenericRecord)
    return Foo(
      a = BooleanDecoder.decode(schema.getField("a").schema(), value.get("a")),
      b = StringDecoder.decode(schema.getField("b").schema(), value.get("b")),
    )
  }
}
""".trim()
   }

})
