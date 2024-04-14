package com.sksamuel.centurion.avro.generation

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder

data class Foo1(val a: String, val b: Boolean, val c: Long)
data class Foo2(val a: String?, val b: Boolean, val c: Long)

class ReflectionSchemaBuilderTest : FunSpec({

   test("basic types") {
      val expected = SchemaBuilder.record("Foo1").namespace(Foo1::class.java.packageName)
         .fields()
         .requiredString("a")
         .requiredBoolean("b")
         .requiredLong("c")
         .endRecord()
      ReflectionSchemaBuilder().schema(Foo1::class) shouldBe expected
   }

   test("nulls") {
      val expected = SchemaBuilder.record("Foo2").namespace(Foo2::class.java.packageName)
         .fields()
         .name("a").type(SchemaBuilder.nullable().stringType()).noDefault()
         .requiredBoolean("b")
         .requiredLong("c")
         .endRecord()
      ReflectionSchemaBuilder().schema(Foo2::class) shouldBe expected
   }

})
