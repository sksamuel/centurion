package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class ListEncoderTest : FunSpec({

   test("encoding list of strings") {
      val schema = Schema.createArray(Schema.create(Schema.Type.STRING))
      ListEncoder(StringEncoder).encode(schema, listOf("foo", "bar")) shouldBe listOf(Utf8("foo"), Utf8("bar"))
   }

   test("encoding nullable list of strings") {
      val schema = ReflectionSchemaBuilder().schema(Fool::class)
      ReflectionRecordEncoder(Fool::class).encode(schema, Fool(null)) shouldBe GenericData.Record(schema)
   }

})

data class Fool(val list: List<Boolean>?)
