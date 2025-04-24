package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class ReflectionRecordEncoderTest : FunSpec({

   test("basic test") {
      data class Foo(val a: String, val b: Boolean)

      val schema = SchemaBuilder.record("Foo").fields().requiredString("a").requiredBoolean("b").endRecord()
      val actual = ReflectionRecordEncoder<Foo>().encode(schema, Foo("hello", true))

      val expected = GenericData.Record(schema)
      expected.put("a", Utf8("hello"))
      expected.put("b", true)

      actual shouldBe expected
   }

   test("enums") {
      data class Foo(val wine: Wine)

      val wineSchema = Schema.createEnum("Wine", null, null, listOf("Shiraz", "Malbec"))
      val schema = SchemaBuilder.record("Foo").fields()
         .name("wine").type(wineSchema).noDefault()
         .endRecord()

      val actual = ReflectionRecordEncoder<Foo>().encode(schema, Foo(Wine.Malbec))

      val expected = GenericData.Record(schema)
      expected.put("wine", GenericData.get().createEnum("Malbec", wineSchema))

      actual shouldBe expected
   }

   test("sets") {
      data class Foo(val set1: Set<Int>, val set2: Set<Long?>, val set3: Set<Wine>)

      val enum = SchemaBuilder.enumeration("Wine").symbols("Malbec", "Shiraz")
      val schema = SchemaBuilder.record("Foo").fields()
         .name("set1").type().array().items().intType().noDefault()
         .name("set2").type().array().items().type(SchemaBuilder.nullable().longType()).noDefault()
         .name("set3").type().array().items().type(enum).noDefault()
         .endRecord()

      val expected = GenericData.Record(schema)
      expected.put("set1", listOf(1, 2))
      expected.put("set2", listOf(1L, null, 2L))
      expected.put(
         "set3",
         listOf(
            GenericData.get().createEnum("Shiraz", enum),
            GenericData.get().createEnum("Malbec", enum),
         )
      )

      ReflectionRecordEncoder<Foo>().encode(
         schema,
         Foo(setOf(1, 2), setOf(1L, null, 2L), setOf(Wine.Shiraz, Wine.Malbec))
      ) shouldBe expected
   }

   test("list") {
      data class Foo(val list1: List<Int>, val list2: List<Long?>, val list3: List<Wine>)

      val enum = SchemaBuilder.enumeration("Wine").symbols("Malbec", "Shiraz")
      val schema = SchemaBuilder.record("Foo").fields()
         .name("list1").type().array().items().intType().noDefault()
         .name("list2").type().array().items().type(SchemaBuilder.nullable().longType()).noDefault()
         .name("list3").type().array().items().type(enum).noDefault()
         .endRecord()

      val expected = GenericData.Record(schema)
      expected.put("list1", listOf(1, 2))
      expected.put("list2", listOf(1L, null, 2L))
      expected.put(
         "list3",
         listOf(
            GenericData.get().createEnum("Shiraz", enum),
            GenericData.get().createEnum("Malbec", enum),
         )
      )

      ReflectionRecordEncoder<Foo>().encode(
         schema,
         Foo(listOf(1, 2), listOf(1L, null, 2L), listOf(Wine.Shiraz, Wine.Malbec))
      ) shouldBe expected
   }

   test("maps") {
      data class Foo(val map: Map<String, Int>)

      val map = mapOf("a" to 1, "b" to 2)

      val schema = SchemaBuilder.record("Foo").namespace(Foo::class.java.packageName)
         .fields()
         .name("map").type().map().values(SchemaBuilder.builder().intType()).noDefault()
         .endRecord()

      val record = GenericData.Record(schema)
      record.put("map", map)

      ReflectionRecordEncoder<Foo>().encode(schema, Foo(map)) shouldBe record
   }

   test("maps of maps") {
      data class Foo(val map: Map<String, Map<String, Int>>)

      val maps = mapOf("a" to mapOf("a" to 1, "b" to 2), "b" to mapOf("a" to 3, "b" to 4))

      val schema = SchemaBuilder.record("Foo").namespace(Foo::class.java.packageName)
         .fields()
         .name("map").type().map().values(SchemaBuilder.builder().map().values().intType()).noDefault()
         .endRecord()

      val record = GenericData.Record(schema)
      record.put("map", maps)

      ReflectionRecordEncoder<Foo>().encode(
         schema, Foo(maps)
      ) shouldBe record
   }
})

enum class Wine { Shiraz, Malbec }
