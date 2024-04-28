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
object MyFooEncoder : Encoder<MyFoo> {

  override fun encode(schema: Schema): (MyFoo) -> GenericRecord {

    val bSchema = schema.getField("b").schema()
    val bPos    = schema.getField("b").pos()
    val bEncode = BooleanEncoder.encode(bSchema)
    val cSchema = schema.getField("c").schema()
    val cPos    = schema.getField("c").pos()
    val cEncode = LongEncoder.encode(cSchema)
    val dSchema = schema.getField("d").schema()
    val dPos    = schema.getField("d").pos()
    val dEncode = DoubleEncoder.encode(dSchema)
    val fSchema = schema.getField("f").schema()
    val fPos    = schema.getField("f").pos()
    val fEncode = FloatEncoder.encode(fSchema)
    val iSchema = schema.getField("i").schema()
    val iPos    = schema.getField("i").pos()
    val iEncode = IntEncoder.encode(iSchema)
    val listsSchema = schema.getField("lists").schema()
    val listsPos    = schema.getField("lists").pos()
    val listsEncode = ListEncoder(IntEncoder).encode(listsSchema)
    val mapsSchema = schema.getField("maps").schema()
    val mapsPos    = schema.getField("maps").pos()
    val mapsEncode = MapEncoder(StringEncoder, DoubleEncoder).encode(mapsSchema)
    val sSchema = schema.getField("s").schema()
    val sPos    = schema.getField("s").pos()
    val sEncode = NullEncoder(StringEncoder).encode(sSchema)
    val setsSchema = schema.getField("sets").schema()
    val setsPos    = schema.getField("sets").pos()
    val setsEncode = SetEncoder(StringEncoder).encode(setsSchema)
    val wineSchema = schema.getField("wine").schema()
    val winePos    = schema.getField("wine").pos()
    val wineEncode = NullEncoder(EnumEncoder()).encode(wineSchema)

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
