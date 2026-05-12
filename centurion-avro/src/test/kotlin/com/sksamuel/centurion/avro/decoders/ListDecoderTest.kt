package com.sksamuel.centurion.avro.decoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.util.Utf8

class ListDecoderTest : FunSpec({

   val stringArraySchema = Schema.createArray(Schema.create(Schema.Type.STRING))

   test("decodes a List of strings") {
      ListDecoder(StringDecoder).decode(stringArraySchema, listOf(Utf8("foo"), Utf8("bar"))) shouldBe listOf("foo", "bar")
   }

   test("decodes an Array of strings") {
      ListDecoder(StringDecoder).decode(stringArraySchema, arrayOf<Any?>(Utf8("a"), Utf8("b"))) shouldBe listOf("a", "b")
   }

   test("preserves order") {
      ListDecoder(StringDecoder).decode(stringArraySchema, listOf(Utf8("c"), Utf8("a"), Utf8("b"))) shouldContainExactly listOf("c", "a", "b")
   }

   test("preserves duplicates") {
      ListDecoder(StringDecoder).decode(stringArraySchema, listOf(Utf8("x"), Utf8("x"))) shouldBe listOf("x", "x")
   }

   test("empty input yields empty list") {
      ListDecoder(StringDecoder).decode(stringArraySchema, emptyList<Any?>()) shouldBe emptyList()
   }

   test("rejects unsupported input") {
      shouldThrow<IllegalStateException> {
         ListDecoder(StringDecoder).decode(stringArraySchema, "not a list")
      }
   }
})
