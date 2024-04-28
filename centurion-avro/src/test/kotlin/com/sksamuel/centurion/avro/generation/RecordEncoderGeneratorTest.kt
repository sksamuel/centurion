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
class MyFooEncoder(schema: Schema) : Encoder<MyFoo> {

  private val bEncoder = BooleanEncoder
  private val bSchema  = schema.getField("b").schema()
  private val bPos     = schema.getField("b").pos()
  private val cEncoder = LongEncoder
  private val cSchema  = schema.getField("c").schema()
  private val cPos     = schema.getField("c").pos()
  private val listsEncoder = ListEncoder(IntEncoder)
  private val listsSchema  = schema.getField("lists").schema()
  private val listsPos     = schema.getField("lists").pos()
  private val mapsEncoder = MapEncoder(StringEncoder, DoubleEncoder)
  private val mapsSchema  = schema.getField("maps").schema()
  private val mapsPos     = schema.getField("maps").pos()
  private val sEncoder = NullEncoder(StringEncoder)
  private val sSchema  = schema.getField("s").schema()
  private val sPos     = schema.getField("s").pos()
  private val setsEncoder = SetEncoder(StringEncoder)
  private val setsSchema  = schema.getField("sets").schema()
  private val setsPos     = schema.getField("sets").pos()
  private val wineEncoder = NullEncoder(EnumEncoder())
  private val wineSchema  = schema.getField("wine").schema()
  private val winePos     = schema.getField("wine").pos()

  override fun encode(schema: Schema, value: MyFoo): GenericRecord {
    val record = GenericData.Record(schema)
    record.put(bPos, bEncoder.encode(bSchema, value.b))
    record.put(cPos, cEncoder.encode(cSchema, value.c))
    record.put(listsPos, listsEncoder.encode(listsSchema, value.lists))
    record.put(mapsPos, mapsEncoder.encode(mapsSchema, value.maps))
    record.put(sPos, sEncoder.encode(sSchema, value.s))
    record.put(setsPos, setsEncoder.encode(setsSchema, value.sets))
    record.put(winePos, wineEncoder.encode(wineSchema, value.wine))
    return record
  }
}
""".trim()
   }

})
