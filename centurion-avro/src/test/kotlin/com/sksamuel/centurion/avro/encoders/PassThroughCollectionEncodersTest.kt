package com.sksamuel.centurion.avro.encoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeSameInstanceAs
import org.apache.avro.Schema

class PassThroughCollectionEncodersTest : FunSpec({

   val arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING))

   test("PassThroughListEncoder returns the input list as-is") {
      val input = listOf<Any?>("a", 1, null)
      val encoded = PassThroughListEncoder.encode(arraySchema, input)
      encoded shouldBeSameInstanceAs input
   }

   test("PassThroughListEncoder accepts an empty list") {
      PassThroughListEncoder.encode(arraySchema, emptyList<Any?>()) shouldBe emptyList()
   }

   test("PassThroughListEncoder requires an ARRAY schema") {
      shouldThrow<IllegalArgumentException> {
         PassThroughListEncoder.encode(Schema.create(Schema.Type.STRING), listOf<Any?>("a"))
      }
   }

   test("PassThroughSetEncoder returns the set as an equivalent List") {
      val input = setOf<Any?>("a", "b")
      val encoded = PassThroughSetEncoder.encode(arraySchema, input)
      encoded.toSet() shouldBe input
   }

   test("PassThroughSetEncoder accepts an empty set") {
      PassThroughSetEncoder.encode(arraySchema, emptySet<Any?>()) shouldBe emptyList()
   }

   test("PassThroughSetEncoder requires an ARRAY schema") {
      shouldThrow<IllegalArgumentException> {
         PassThroughSetEncoder.encode(Schema.create(Schema.Type.STRING), setOf<Any?>("a"))
      }
   }
})
