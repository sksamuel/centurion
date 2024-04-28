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

  private val bEncoder = BooleanEncoder
  private val cEncoder = LongEncoder
  private val listsEncoder = ListEncoder(IntEncoder)
  private val mapsEncoder = MapEncoder(StringEncoder, DoubleEncoder)
  private val sEncoder = NullEncoder(StringEncoder)
  private val setsEncoder = SetEncoder(StringEncoder)
  private val wineEncoder = NullEncoder(EnumEncoder())

  override fun encode(schema: Schema, value: MyFoo): GenericRecord {
    val record = GenericData.Record(schema)
    record.put("b", bEncoder.encode(schema.getField("b").schema(), value.b))
    record.put("c", cEncoder.encode(schema.getField("c").schema(), value.c))
    record.put("lists", listsEncoder.encode(schema.getField("lists").schema(), value.lists))
    record.put("maps", mapsEncoder.encode(schema.getField("maps").schema(), value.maps))
    record.put("s", sEncoder.encode(schema.getField("s").schema(), value.s))
    record.put("sets", setsEncoder.encode(schema.getField("sets").schema(), value.sets))
    record.put("wine", wineEncoder.encode(schema.getField("wine").schema(), value.wine))
    return record
  }
}
""".trim()
   }

})
