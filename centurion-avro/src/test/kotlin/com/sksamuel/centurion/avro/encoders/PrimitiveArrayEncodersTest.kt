package com.sksamuel.centurion.avro.encoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema

class PrimitiveArrayEncodersTest : FunSpec({

   val intArraySchema = Schema.createArray(Schema.create(Schema.Type.INT))
   val longArraySchema = Schema.createArray(Schema.create(Schema.Type.LONG))

   test("IntArrayEncoder encodes an IntArray as a List<Int>") {
      IntArrayEncoder().encode(intArraySchema, intArrayOf(1, 2, 3)) shouldBe listOf(1, 2, 3)
   }

   test("IntArrayEncoder encodes an empty IntArray as an empty list") {
      IntArrayEncoder().encode(intArraySchema, intArrayOf()) shouldBe emptyList()
   }

   test("IntArrayEncoder requires an ARRAY schema") {
      shouldThrow<IllegalArgumentException> {
         IntArrayEncoder().encode(Schema.create(Schema.Type.INT), intArrayOf(1))
      }
   }

   test("LongArrayEncoder encodes a LongArray as a List<Long>") {
      LongArrayEncoder().encode(longArraySchema, longArrayOf(10L, 20L, 30L)) shouldBe listOf(10L, 20L, 30L)
   }

   test("LongArrayEncoder encodes an empty LongArray as an empty list") {
      LongArrayEncoder().encode(longArraySchema, longArrayOf()) shouldBe emptyList()
   }

   test("LongArrayEncoder requires an ARRAY schema") {
      shouldThrow<IllegalArgumentException> {
         LongArrayEncoder().encode(Schema.create(Schema.Type.LONG), longArrayOf(1L))
      }
   }
})
