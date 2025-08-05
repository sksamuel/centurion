package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.generic.GenericData

data class Foo1(val map: Map<String, Boolean>)
data class Foo2(val map: Map<String, Boolean>?)

class MapDecoderTest : FunSpec({
   test("map") {

      val schema = ReflectionSchemaBuilder().schema(Foo1::class)
      val encoder = ReflectionRecordDecoder(Foo1::class)

      val record = GenericData.Record(schema)
      record.put("map", mapOf("a" to true, "b" to false))

      val expected = Foo1(mapOf("a" to true, "b" to false))

      encoder.decode(schema, record) shouldBe expected
   }


   test("nullable map") {

      val schema = ReflectionSchemaBuilder().schema(Foo2::class)
      val encoder = ReflectionRecordDecoder(Foo2::class)

      val record = GenericData.Record(schema)
      record.put("map", null)

      encoder.decode(schema, record) shouldBe Foo2(null)
   }
})
