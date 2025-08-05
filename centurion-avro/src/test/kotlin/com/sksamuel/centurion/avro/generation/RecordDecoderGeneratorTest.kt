//package com.sksamuel.centurion.avro.generation
//
//import io.kotest.core.spec.style.FunSpec
//import io.kotest.matchers.shouldBe
//
//class RecordDecoderGeneratorTest : FunSpec({
//
//   test("simple decoder") {
//      RecordDecoderGenerator().generate(MyFoo::class).trim() shouldBe """
//package com.sksamuel.centurion.avro.generation
//
//import com.sksamuel.centurion.avro.decoders.*
//import org.apache.avro.Schema
//import org.apache.avro.generic.GenericData
//import org.apache.avro.generic.GenericRecord
//
///**
// * This is a generated [Decoder] that deserializes Avro [GenericRecord]s to [MyFoo]s
// */
//object MyFooDecoder : Decoder<MyFoo> {
//
//  override fun decode(schema: Schema): (Any?) -> MyFoo {
//
//    val bSchema = schema.getField("b").schema()
//    val bPos    = schema.getField("b").pos()
//    val bDecode = BooleanDecoder.decode(bSchema)
//    val cSchema = schema.getField("c").schema()
//    val cPos    = schema.getField("c").pos()
//    val cDecode = LongDecoder.decode(cSchema)
//    val dSchema = schema.getField("d").schema()
//    val dPos    = schema.getField("d").pos()
//    val dDecode = DoubleDecoder.decode(dSchema)
//    val fSchema = schema.getField("f").schema()
//    val fPos    = schema.getField("f").pos()
//    val fDecode = FloatDecoder.decode(fSchema)
//    val iSchema = schema.getField("i").schema()
//    val iPos    = schema.getField("i").pos()
//    val iDecode = IntDecoder.decode(iSchema)
//    val listsSchema = schema.getField("lists").schema()
//    val listsPos    = schema.getField("lists").pos()
//    val listsDecode = ListDecoder(IntDecoder).decode(listsSchema)
//    val mapsSchema = schema.getField("maps").schema()
//    val mapsPos    = schema.getField("maps").pos()
//    val mapsDecode = MapDecoder(DoubleDecoder).decode(mapsSchema)
//    val sSchema = schema.getField("s").schema()
//    val sPos    = schema.getField("s").pos()
//    val sDecode = NullDecoder(StringDecoder).decode(sSchema)
//    val setsSchema = schema.getField("sets").schema()
//    val setsPos    = schema.getField("sets").pos()
//    val setsDecode = SetDecoder(StringDecoder).decode(setsSchema)
//    val wineSchema = schema.getField("wine").schema()
//    val winePos    = schema.getField("wine").pos()
//    val wineDecode = NullDecoder(EnumDecoder<com.sksamuel.centurion.avro.encoders.Wine>()).decode(wineSchema)
//
//    return { record ->
//      require(record is GenericRecord)
//      MyFoo(
//        b = bDecode(record[bPos]),
//        c = cDecode(record[cPos]),
//        d = dDecode(record[dPos]),
//        f = fDecode(record[fPos]),
//        i = iDecode(record[iPos]),
//        lists = listsDecode(record[listsPos]),
//        maps = mapsDecode(record[mapsPos]),
//        s = sDecode(record[sPos]),
//        sets = setsDecode(record[setsPos]),
//        wine = wineDecode(record[winePos]),
//      )
//    }
//  }
//}
//""".trim()
//   }
//
//})
