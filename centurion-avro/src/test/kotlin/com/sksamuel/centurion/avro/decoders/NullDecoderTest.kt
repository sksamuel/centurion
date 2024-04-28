package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder

class NullDecoderTest : FunSpec({

   test("decode null") {
      val schema = SchemaBuilder.nullable().intType()
      NullDecoder(IntDecoder).decode(schema).invoke(null) shouldBe null
      NullDecoder(IntDecoder).decode(schema).invoke(1) shouldBe 1
   }

})
