package com.sksamuel.centurion.avro

import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.encoders.Wine
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import java.nio.ByteBuffer

class RoundTripTest : FunSpec() {
   init {
      test("round trip encode / decode") {
         val schema = ReflectionSchemaBuilder().schema(RoundTrip::class)
         val before = RoundTrip(
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
            ba = byteArrayOf(1, 2, 3),
            bb = ByteBuffer.wrap(byteArrayOf(1, 2, 3))
         )

         val after = ReflectionRecordDecoder<RoundTrip>(schema).decode(
            schema,
            ReflectionRecordEncoder<RoundTrip>(schema).encode(schema, before)
         )

         after.s shouldBe before.s
         after.b shouldBe before.b
         after.l shouldBe before.l
         after.d shouldBe before.d
         after.i shouldBe before.i
         after.f shouldBe before.f
         after.sets shouldBe before.sets
         after.lists shouldBe before.lists
         after.maps shouldBe before.maps
         after.wine shouldBe before.wine
         after.arrays shouldBe before.arrays
         after.ba shouldBe before.ba
         after.bb shouldBe "qwe"
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
   val ba: ByteArray,
   val bb: ByteBuffer,
)
