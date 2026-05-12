package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.util.Utf8

class SetDecoderTest : FunSpec({

   test("decoding a List of strings") {
      val schema = Schema.createArray(Schema.create(Schema.Type.STRING))
      SetDecoder(StringDecoder).decode(schema, listOf(Utf8("foo"), Utf8("bar"))) shouldBe setOf("foo", "bar")
   }

   test("decoding an Array of strings") {
      val schema = Schema.createArray(Schema.create(Schema.Type.STRING))
      SetDecoder(StringDecoder).decode(schema, arrayOf<Any?>(Utf8("a"), Utf8("b"))) shouldBe setOf("a", "b")
   }

   test("preserves encounter order") {
      val schema = Schema.createArray(Schema.create(Schema.Type.STRING))
      val decoded = SetDecoder(StringDecoder).decode(schema, listOf(Utf8("c"), Utf8("a"), Utf8("b")))
      decoded.toList() shouldContainExactly listOf("c", "a", "b")
   }

   test("deduplicates repeated elements") {
      val schema = Schema.createArray(Schema.create(Schema.Type.STRING))
      SetDecoder(StringDecoder).decode(schema, listOf(Utf8("x"), Utf8("x"), Utf8("y"))) shouldBe setOf("x", "y")
   }

   test("empty list yields empty set") {
      val schema = Schema.createArray(Schema.create(Schema.Type.STRING))
      SetDecoder(StringDecoder).decode(schema, emptyList<Any?>()) shouldBe emptySet()
   }
})
