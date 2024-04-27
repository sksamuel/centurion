package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.EnumSymbol

class EnumEncoderTest : FunSpec({

   test("use EnumSymbol") {
      val schema = Schema.createEnum("Wine", null, null, listOf("Shiraz", "Malbec"))
      EnumEncoder().encode(schema, Wine.Shiraz) shouldBe EnumSymbol(schema, "Shiraz")
   }

   test("detect globalUseJavaStringForEnum") {
      Encoder.globalUseJavaStringForEnum = true
      val schema = Schema.createEnum("Wine", null, null, listOf("Shiraz", "Malbec"))
      EnumEncoder().encode(schema, Wine.Shiraz) shouldBe "Shiraz"
      Encoder.globalUseJavaStringForEnum = false
   }

})
