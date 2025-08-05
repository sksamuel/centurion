package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class SetEncoderTest : FunSpec({

   test("encoding set of strings") {
      val schema = Schema.createArray(Schema.create(Schema.Type.STRING))
      SetEncoder(StringEncoder).encode(schema, setOf("foo", "bar")) shouldBe listOf(Utf8("foo"), Utf8("bar"))
   }

   test("encoding nullable set of strings") {
      val schema = ReflectionSchemaBuilder().schema(FooS::class)
      ReflectionRecordEncoder(FooS::class).encode(schema, FooS(null)) shouldBe GenericData.Record(schema)
   }
})

data class FooS(val set: Set<Boolean>?)
