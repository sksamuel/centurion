package com.sksamuel.centurion.avro

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.decoders.SpecificRecordDecoder
import com.sksamuel.centurion.avro.encoders.SpecificRecordEncoder
import com.sksamuel.centurion.avro.encoders.Wine
import com.sksamuel.centurion.avro.generation.ReflectionSchemaBuilder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class RoundTripTest : FunSpec() {
   init {
      test("round trip encode / decode") {
         Decoder.useStrictPrimitiveDecoders = true
         val schema = ReflectionSchemaBuilder().schema(RoundTrip::class)
         val rt = RoundTrip(
            s = null,
            b = false,
            l = 1436,
            d = 4.5,
            i = 7799,
            f = 6.7f,
            sets = setOf("foo", "bar"),
            lists = listOf(6, 7),
            arrays = longArrayOf(6L, 7L),
            maps = mapOf(),
            wine = Wine.Shiraz,
         )
         val actual = SpecificRecordDecoder(RoundTrip::class).decode(schema)
            .invoke(SpecificRecordEncoder(RoundTrip::class).encode(schema).invoke(rt))
         actual.s shouldBe actual.s
         actual.b shouldBe actual.b
         actual.l shouldBe actual.l
         actual.d shouldBe actual.d
         actual.i shouldBe actual.i
         actual.f shouldBe actual.f
         actual.sets shouldBe actual.sets
         actual.lists shouldBe actual.lists
         actual.maps shouldBe actual.maps
         actual.wine shouldBe actual.wine
         actual.arrays shouldBe actual.arrays
         Decoder.useStrictPrimitiveDecoders = false
      }
   }
}

data class RoundTrip(
   val s: String?,
   val b: Boolean,
   val l: Long,
   val d: Double,
   val i: Int,
   val f: Float,
   val sets: Set<String>,
   val lists: List<Int>,
   val arrays: LongArray,
   val maps: Map<String, Double>,
   val wine: Wine?,
)
