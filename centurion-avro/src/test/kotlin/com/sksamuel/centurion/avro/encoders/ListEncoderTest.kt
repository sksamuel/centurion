package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.util.Utf8

class ListEncoderTest : FunSpec({

   test("encoding list of strings") {
      val schema = Schema.createArray(Schema.create(Schema.Type.STRING))
      ListEncoder(StringEncoder).encode(schema).invoke(listOf("foo", "bar")) shouldBe listOf(Utf8("foo"), Utf8("bar"))
   }

})

