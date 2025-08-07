package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.generic.GenericData

data class Foo1(val map: Map<String, Boolean>)
data class Foo2(val map: Map<String, Boolean>?)

class MapEncoderTest : FunSpec({

   test("map") {

      val schema = ReflectionSchemaBuilder().schema(Foo1::class)
      val encoder = ReflectionRecordEncoder(Foo1::class)

      val expected = GenericData.Record(schema)
      expected.put("map", mapOf("a" to true, "b" to false))

      encoder.encode(schema, Foo1(mapOf("a" to true, "b" to false))) shouldBe expected
   }

   test("nullable map") {

      val schema = ReflectionSchemaBuilder().schema(Foo2::class)
      val encoder = ReflectionRecordEncoder(Foo2::class)

      val expected = GenericData.Record(schema)
      expected.put("map", null)

      encoder.encode(schema, Foo2(null)) shouldBe expected
   }
})
