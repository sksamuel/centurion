package com.sksamuel.centurion.avro.generation


import com.sksamuel.centurion.avro.encoders.Wine
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class RecordEncoderGeneratorTest : FunSpec({

   data class MyFoo(
      val b: Boolean,
      val s: String?,
      val c: Long,
      val sets: Set<String>,
      val lists: List<Int>,
      val maps: Map<String, Double>,
      val wine: Wine?,
   )

   test("simple encoder") {
      RecordEncoderGenerator().generate(MyFoo::class).trim() shouldBe """
package com.sksamuel.centurion.avro.generation

import com.sksamuel.centurion.avro.encoders.*
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

/**
 * This is a generated [Encoder] that encodes [MyFoo]s to Avro [GenericRecord]s
 */
object MyFooEncoder : Encoder<MyFoo> {
  override fun encode(schema: Schema, value: MyFoo): GenericRecord {
    val record = GenericData.Record(schema)
    record.put("b", BooleanEncoder.encode(schema.getField("b").schema(), value.b))
    record.put("c", LongEncoder.encode(schema.getField("c").schema(), value.c))
    record.put("lists", ListEncoder(IntEncoder).encode(schema.getField("lists").schema(), value.lists))
    record.put("maps", MapEncoder(StringEncoder, DoubleEncoder).encode(schema.getField("maps").schema(), value.maps))
    record.put("s", NullEncoder(StringEncoder).encode(schema.getField("s").schema(), value.s))
    record.put("sets", SetEncoder(StringEncoder).encode(schema.getField("sets").schema(), value.sets))
    record.put("wine", NullEncoder(EnumEncoder()).encode(schema.getField("wine").schema(), value.wine))
    return record
  }
}
""".trim()
   }

})


