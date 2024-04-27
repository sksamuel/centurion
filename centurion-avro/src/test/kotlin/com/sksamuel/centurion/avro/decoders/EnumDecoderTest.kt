package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class EnumDecoderTest : FunSpec({

   val schema = Schema.createEnum("W", null, null, listOf("Wobble", "Wibble"))

   test("support GenericEnumSymbol") {
      EnumDecoder(W::class).decode(schema, GenericData.get().createEnum("Wibble", schema)) shouldBe W.Wibble
   }

   test("support strings") {
      EnumDecoder(W::class).decode(schema, "Wibble") shouldBe W.Wibble
   }

   test("support UTF8s") {
      EnumDecoder(W::class).decode(schema, Utf8("Wibble")) shouldBe W.Wibble
   }

})

private enum class W { Wibble, Wobble }
