//package com.sksamuel.centurion.avro.decoders
//
//import com.sksamuel.centurion.avro.encoders.Wine
//import com.sksamuel.centurion.avro.schemas.Foo10
//import com.sksamuel.centurion.avro.schemas.Foo11
//import com.sksamuel.centurion.avro.schemas.Foo12
//import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
//import io.kotest.core.spec.style.FunSpec
//import io.kotest.matchers.shouldBe
//import org.apache.avro.Schema
//import org.apache.avro.SchemaBuilder
//import org.apache.avro.generic.GenericData
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.util.Utf8
//
//class ReflectionRecordDecoderTest : FunSpec({
//
//   test("basic record decoder") {
//      data class Foo(val a: String, val b: Boolean)
//
//      val schema = SchemaBuilder.record("Foo").fields().requiredString("a").requiredBoolean("b").endRecord()
//
//      val record = GenericData.Record(schema)
//      record.put("a", Utf8("hello"))
//      record.put("b", true)
//
//      ReflectionRecordDecoder<Foo>(schema).decode(schema, record) shouldBe Foo("hello", true)
//   }
//
//   test("enums") {
//      data class Foo(val wine: Wine)
//
//      val wineSchema = Schema.createEnum("Wine", null, null, listOf("Shiraz", "Malbec"))
//      val schema = SchemaBuilder.record("Foo").fields()
//         .name("wine").type(wineSchema).noDefault()
//         .endRecord()
//
//      val record = GenericData.Record(schema)
//      record.put("wine", GenericData.get().createEnum("Shiraz", wineSchema))
//
//      ReflectionRecordDecoder<Foo>(schema).decode(schema, record) shouldBe Foo(Wine.Shiraz)
//   }
//
//   test("nulls") {
//      data class Foo(val a: String?, val b: String)
//
//      val schema = SchemaBuilder.record("Foo").fields()
//         .optionalString("a")
//         .requiredString("b")
//         .endRecord()
//
//      val record = GenericData.Record(schema)
//      record.put("a", null)
//      record.put("b", Utf8("hello"))
//
//      ReflectionRecordDecoder<Foo>(schema).decode(schema, record) shouldBe Foo(null, "hello")
//   }
//
//   test("sets") {
//      data class Foo(val set1: Set<Int>, val set2: Set<Long?>, val set3: Set<Wine>)
//
//      val enum = SchemaBuilder.enumeration("Wine").symbols("Malbec", "Shiraz")
//      val schema = SchemaBuilder.record("Foo").fields()
//         .name("set1").type().array().items().intType().noDefault()
//         .name("set2").type().array().items().type(SchemaBuilder.nullable().longType()).noDefault()
//         .name("set3").type().array().items().type(enum).noDefault()
//         .endRecord()
//
//      val record = GenericData.Record(schema)
//      record.put("set1", listOf(1, 2))
//      record.put("set2", listOf(1L, null, 2L))
//      record.put(
//         "set3",
//         listOf(
//            GenericData.get().createEnum("Shiraz", enum),
//            GenericData.get().createEnum("Malbec", enum),
//         )
//      )
//
//      ReflectionRecordDecoder<Foo>(schema).decode(schema, record) shouldBe Foo(
//         setOf(1, 2),
//         setOf(1L, null, 2L),
//         setOf(Wine.Shiraz, Wine.Malbec),
//      )
//   }
//
//   test("list") {
//      data class Foo(val list1: List<Int>, val list2: List<Long?>, val list3: List<Wine>)
//
//      val enum = SchemaBuilder.enumeration("Wine").symbols("Malbec", "Shiraz")
//      val schema = SchemaBuilder.record("Foo").fields()
//         .name("list1").type().array().items().intType().noDefault()
//         .name("list2").type().array().items().type(SchemaBuilder.nullable().longType()).noDefault()
//         .name("list3").type().array().items().type(enum).noDefault()
//         .endRecord()
//
//      val record = GenericData.Record(schema)
//      record.put("list1", listOf(1, 2))
//      record.put("list2", listOf(1L, null, 2L))
//      record.put(
//         "list3",
//         listOf(
//            GenericData.get().createEnum("Shiraz", enum),
//            GenericData.get().createEnum("Malbec", enum),
//         )
//      )
//
//      ReflectionRecordDecoder<Foo>(schema).decode(schema, record) shouldBe Foo(
//         listOf(1, 2),
//         listOf(1L, null, 2L),
//         listOf(Wine.Shiraz, Wine.Malbec),
//      )
//   }
//
//   test("maps") {
//      data class Foo(val map: Map<String, Int>)
//
//      val schema = SchemaBuilder.record("Foo").namespace(Foo::class.java.packageName)
//         .fields()
//         .name("map").type().map().values().intType().noDefault()
//         .endRecord()
//
//      val map = mapOf("a" to 1, "b" to 2)
//
//      val record = GenericData.Record(schema)
//      record.put("map", map)
//
//      ReflectionRecordDecoder<Foo>(schema).decode(schema, record) shouldBe Foo(map)
//   }
//
//   test("maps using UTF8") {
//      data class Foo(val map: Map<String, Int>)
//
//      val schema = SchemaBuilder.record("Foo").namespace(Foo::class.java.packageName)
//         .fields()
//         .name("map").type().map().values().intType().noDefault()
//         .endRecord()
//
//      val map = mapOf(Utf8("a") to 1, "b" to 2)
//
//      val record = GenericData.Record(schema)
//      record.put("map", map)
//
//      ReflectionRecordDecoder<Foo>(schema).decode(schema, record) shouldBe Foo(mapOf("a" to 1, "b" to 2))
//   }
//
//   test("maps of maps") {
//      data class Foo(val map: Map<String, Map<String, Int>>)
//
//      val schema = SchemaBuilder.record("Foo").namespace(Foo::class.java.packageName)
//         .fields()
//         .name("map").type().map().values(SchemaBuilder.builder().map().values().intType()).noDefault()
//         .endRecord()
//
//      val maps = mapOf("a" to mapOf("a" to 1, "b" to 2), "b" to mapOf("a" to 3, "b" to 4))
//
//      val record = GenericData.Record(schema)
//      record.put("map", maps)
//
//      ReflectionRecordDecoder<Foo>(schema).decode(schema, record) shouldBe Foo(maps)
//   }
//
//   test("records of lists of records") {
//
//      val schema = ReflectionSchemaBuilder().schema(Foo10::class)
//      val b = schema.getField("b").schema()
//
//      val record = GenericData.Record(schema)
//      record.put("a", "hello")
//      record.put("b", GenericData.Array<GenericRecord>(2, b).also { array ->
//         array.add(GenericData.Record(b.elementType).also { it.put("b", true) })
//         array.add(GenericData.Record(b.elementType).also { it.put("b", false) })
//      })
//
//      ReflectionRecordDecoder<Foo10>(schema).decode(
//         schema,
//         record
//      ) shouldBe Foo10(a = "hello", b = listOf(Foo11(b = true), Foo11(b = false)))
//
//   }
//
//   test("records of sets of records") {
//
//      val schema = ReflectionSchemaBuilder().schema(Foo12::class)
//      val b = schema.getField("b").schema()
//
//      val record = GenericData.Record(schema)
//      record.put("a", "hello")
//      record.put("b", GenericData.Array<GenericRecord>(2, b).also { array ->
//         array.add(GenericData.Record(b.elementType).also { it.put("b", true) })
//         array.add(GenericData.Record(b.elementType).also { it.put("b", false) })
//      })
//
//      ReflectionRecordDecoder<Foo12>(schema).decode(
//         schema,
//         record
//      ) shouldBe Foo12(a = "hello", b = setOf(Foo11(b = true), Foo11(b = false)))
//
//   }
//})
