package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.util.Utf8

class StringTypeDecoderTest : FunSpec({

   val stringSchema = Schema.create(Schema.Type.STRING)

   test("StringTypeDecoder decodes a Kotlin String") {
      StringTypeDecoder.decode(stringSchema, "hello") shouldBe "hello"
   }

   test("StringTypeDecoder decodes a Utf8 via toString") {
      StringTypeDecoder.decode(stringSchema, Utf8("world")) shouldBe "world"
   }

   test("StringTypeDecoder calls toString on arbitrary CharSequence input") {
      val cs: CharSequence = StringBuilder("abc")
      StringTypeDecoder.decode(stringSchema, cs) shouldBe "abc"
   }

   test("StringTypeDecoder calls toString on non-CharSequence input") {
      StringTypeDecoder.decode(stringSchema, 12345) shouldBe "12345"
   }
})
