package com.sksamuel.centurion.avro.generation


import com.sksamuel.centurion.avro.encoders.Wine
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

data class MyFoo(
   val b: Boolean,
   val s: String?,
   val c: Long,
   val d: Double,
   val i: Int,
   val f: Float,
   val sets: Set<String>,
   val lists: List<Int>,
   val maps: Map<String, Double>,
   val wine: Wine?,
)

class RecordEncoderGeneratorTest : FunSpec({

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

  private val bSchema = schema.getField("b").schema()
  private val bPos    = schema.getField("b").pos()
  private val bEncode = BooleanEncoder.encode(bSchema)
  private val cSchema = schema.getField("c").schema()
  private val cPos    = schema.getField("c").pos()
  private val cEncode = LongEncoder.encode(cSchema)
  private val dSchema = schema.getField("d").schema()
  private val dPos    = schema.getField("d").pos()
  private val dEncode = DoubleEncoder.encode(dSchema)
  private val fSchema = schema.getField("f").schema()
  private val fPos    = schema.getField("f").pos()
  private val fEncode = FloatEncoder.encode(fSchema)
  private val iSchema = schema.getField("i").schema()
  private val iPos    = schema.getField("i").pos()
  private val iEncode = IntEncoder.encode(iSchema)
  private val listsSchema = schema.getField("lists").schema()
  private val listsPos    = schema.getField("lists").pos()
  private val listsEncode = ListEncoder(IntEncoder).encode(listsSchema)
  private val mapsSchema = schema.getField("maps").schema()
  private val mapsPos    = schema.getField("maps").pos()
  private val mapsEncode = MapEncoder(StringEncoder, DoubleEncoder).encode(mapsSchema)
  private val sSchema = schema.getField("s").schema()
  private val sPos    = schema.getField("s").pos()
  private val sEncode = NullEncoder(StringEncoder).encode(sSchema)
  private val setsSchema = schema.getField("sets").schema()
  private val setsPos    = schema.getField("sets").pos()
  private val setsEncode = SetEncoder(StringEncoder).encode(setsSchema)
  private val wineSchema = schema.getField("wine").schema()
  private val winePos    = schema.getField("wine").pos()
  private val wineEncode = NullEncoder(EnumEncoder()).encode(wineSchema)

  override fun encode(schema: Schema): (MyFoo) -> GenericRecord {
    return { value ->
      val record = GenericData.Record(schema)
      record.put(bPos, value.b)
      record.put(cPos, value.c)
      record.put(dPos, value.d)
      record.put(fPos, value.f)
      record.put(iPos, value.i)
      record.put(listsPos, listsEncode.invoke(value.lists))
      record.put(mapsPos, mapsEncode.invoke(value.maps))
      record.put(sPos, value.s)
      record.put(setsPos, setsEncode.invoke(value.sets))
      record.put(winePos, wineEncode.invoke(value.wine))
      record
    }
  }
}
""".trim()
   }

})
