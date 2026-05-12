package com.sksamuel.centurion.avro.decoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema

class ArrayDecodersTest : FunSpec({

   val intArraySchema = Schema.createArray(Schema.create(Schema.Type.INT))
   val longArraySchema = Schema.createArray(Schema.create(Schema.Type.LONG))

   test("IntArrayDecoder decodes a List<Int>") {
      IntArrayDecoder(IntDecoder).decode(intArraySchema, listOf(1, 2, 3)) shouldBe intArrayOf(1, 2, 3)
   }

   test("IntArrayDecoder decodes an Array<Int>") {
      IntArrayDecoder(IntDecoder).decode(intArraySchema, arrayOf<Any?>(4, 5, 6)) shouldBe intArrayOf(4, 5, 6)
   }

   test("IntArrayDecoder accepts widening from Byte/Short via IntDecoder") {
      IntArrayDecoder(IntDecoder).decode(intArraySchema, listOf(1.toByte(), 2.toShort(), 3)) shouldBe intArrayOf(1, 2, 3)
   }

   test("IntArrayDecoder rejects an unsupported value") {
      shouldThrow<IllegalStateException> {
         IntArrayDecoder(IntDecoder).decode(intArraySchema, "not a list")
      }
   }

   test("LongArrayDecoder decodes a List<Long>") {
      LongArrayDecoder(LongDecoder).decode(longArraySchema, listOf(10L, 20L)) shouldBe longArrayOf(10L, 20L)
   }

   test("LongArrayDecoder decodes an Array<Long>") {
      LongArrayDecoder(LongDecoder).decode(longArraySchema, arrayOf<Any?>(40L, 50L)) shouldBe longArrayOf(40L, 50L)
   }

   test("LongArrayDecoder widens Int to Long via LongDecoder") {
      LongArrayDecoder(LongDecoder).decode(longArraySchema, listOf(1, 2, 3)) shouldBe longArrayOf(1L, 2L, 3L)
   }

   test("PassthroughListDecoder returns the input List as-is") {
      val input = listOf<Any?>("a", 1, null)
      PassthroughListDecoder.decode(intArraySchema, input) shouldBe input
   }

   test("PassthroughListDecoder turns an Array into a List") {
      val input = arrayOf<Any?>("a", "b")
      PassthroughListDecoder.decode(intArraySchema, input) shouldBe listOf("a", "b")
   }

   test("PassthroughListDecoder handles other Collection types") {
      val decoded = PassthroughListDecoder.decode(intArraySchema, setOf("a", "b"))
      decoded.toSet() shouldBe setOf("a", "b")
   }

   test("PassthroughSetDecoder turns a List into a Set") {
      PassthroughSetDecoder.decode(intArraySchema, listOf("x", "y", "x")) shouldBe setOf("x", "y")
   }

   test("PassthroughSetDecoder turns an Array into a Set") {
      PassthroughSetDecoder.decode(intArraySchema, arrayOf<Any?>(1, 2, 1)) shouldBe setOf(1, 2)
   }
})
