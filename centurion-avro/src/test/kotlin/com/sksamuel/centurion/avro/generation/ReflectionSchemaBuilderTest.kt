package com.sksamuel.centurion.avro.generation

import com.sksamuel.centurion.avro.encoders.Wine
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder


data class Foo1(val a: String, val b: Boolean, val c: Long)
data class Foo2(val a: String?, val b: Boolean, val c: Long)
data class Foo3(val set1: Set<Int>, val set2: Set<Int?>)
data class Foo4(val list1: List<Int>, val list2: List<Int?>)
data class Foo5(val wine: Wine)

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

   test("sets") {
      val expected = SchemaBuilder.record("Foo3").namespace(Foo3::class.java.packageName)
         .fields()
         .name("set1").type(SchemaBuilder.array().items().intType()).noDefault()
         .name("set2").type(SchemaBuilder.array().items(SchemaBuilder.nullable().intType())).noDefault()
         .endRecord()
      ReflectionSchemaBuilder().schema(Foo3::class) shouldBe expected
   }

   test("lists") {
      val expected = SchemaBuilder.record("Foo4").namespace(Foo4::class.java.packageName)
         .fields()
         .name("list1").type(SchemaBuilder.array().items().intType()).noDefault()
         .name("list2").type(SchemaBuilder.array().items(SchemaBuilder.nullable().intType())).noDefault()
         .endRecord()
      ReflectionSchemaBuilder().schema(Foo4::class) shouldBe expected
   }

   test("enums") {
      val expected = SchemaBuilder.record("Foo5").namespace(Foo5::class.java.packageName)
         .fields()
         .name("wine").type().enumeration("Wine").namespace(Wine::class.java.packageName).symbols("Shiraz", "Malbec")
         .noDefault()
         .endRecord()
      ReflectionSchemaBuilder().schema(Foo5::class) shouldBe expected
   }
})
