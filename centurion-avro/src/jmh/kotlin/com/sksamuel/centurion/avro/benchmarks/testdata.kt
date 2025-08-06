package com.sksamuel.centurion.avro.benchmarks

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryWriter
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import kotlin.random.Random

val longArraySchema: Schema = SchemaBuilder.array().items().longType()
val stringArraySchema: Schema = SchemaBuilder.array().items().stringType()
val enumSchema: Schema = SchemaBuilder.enumeration("enum").symbols("Foo", "Bar", "Baz")

val barSchema: Schema = SchemaBuilder.record("bar").fields()
   .requiredString("field_a")
   .requiredBoolean("field_b")
   .requiredInt("field_c")
   .requiredDouble("field_d")
   .endRecord()

val schema: Schema = SchemaBuilder.record("foo").fields()
   .requiredString("field_a")
   .requiredBoolean("field_b")
   .requiredInt("field_c")
   .requiredDouble("field_d")
   .requiredInt("field_e")
   .requiredString("field_f")
   .requiredString("field_g")
   .requiredInt("field_h")
   .name("enum").type(enumSchema).noDefault()
   .name("listOfEnums").type(SchemaBuilder.array().items(enumSchema)).noDefault()
   .name("setOfEnums").type(SchemaBuilder.array().items(enumSchema)).noDefault()
   .name("listOfLongs").type(longArraySchema).noDefault()
   .name("setOfStrings").type(stringArraySchema).noDefault()
   .name("complexList").type(SchemaBuilder.array().items(barSchema)).noDefault()
   .endRecord()

data class Foo(
   val field_a: String,
   val field_b: Boolean,
   val field_c: Int,
   val field_d: Double,
   val field_e: Int,
   val field_f: String,
   val field_g: String,
   val field_h: Int,
   val enum: Enum,
   val listOfEnums: List<Enum>,
   val setOfEnums: Set<Enum>,
   val listOfLongs: List<Long>,
   val setOfStrings: Set<String>,
   val complexList: List<Bar>,
)

data class Bar(
   val field_a: String,
   val field_b: Boolean,
   val field_c: Int,
   val field_d: Double
)

enum class Enum {
   Foo, Bar, Baz
}

fun createFoo(): Foo {
   val foo = Foo(
      field_a = "hello world",
      field_b = true,
      field_c = 123456,
      field_d = 56.331,
      field_e = 998876324,
      field_f = "stringy mcstring face",
      field_g = "another string",
      field_h = 821377124,
      enum = Enum.Foo,
      listOfEnums = List(100) { Enum.entries.shuffled().first() },
      setOfEnums = List(100) { Enum.entries.shuffled().first() }.toSet(),
      listOfLongs = createListOfLongs(),
      setOfStrings = createRandomStrings().toSet(),
      complexList = List(100) {
         Bar(
            field_a = "bar string $it",
            field_b = it % 2 == 0,
            field_c = it * 1000,
            field_d = it.toDouble() / 3.14
         )
      }
   )
   return foo
}

fun createListOfLongs(): List<Long> {
   return List(600) { Random.nextLong() }
}

fun createRandomStrings(): List<String> {
   val charPool = ('a'..'z') + ('A'..'Z') + ('0'..'9')
   return List(100) {
      val length = Random.nextInt(10, 31) // Between 10 and 30 characters
      (1..length)
         .map { Random.nextInt(0, charPool.size) }
         .map(charPool::get)
         .joinToString("")
   }
}

fun createJson(foo: Foo): ByteArray {
   val mapper = jacksonObjectMapper()
   return mapper.writeValueAsBytes(foo)
}

fun createAvroBytes(foo: Foo): ByteArray {
   val baos = ByteArrayOutputStream()
   val encoder = ReflectionRecordEncoder<Foo>(schema)
   BinaryWriter(
      schema = schema,
      out = baos,
      ef = EncoderFactory.get(),
      encoder = encoder,
      reuse = null
   ).use { it.write(foo) }

   return baos.toByteArray()
}

fun createRecordProgramatically(foo: Foo): GenericData.Record {
   val record = GenericData.Record(schema)
   record.put("field_a", foo.field_a)
   record.put("field_b", foo.field_b)
   record.put("field_c", foo.field_c)
   record.put("field_d", foo.field_d)
   record.put("field_e", foo.field_e)
   record.put("field_f", foo.field_f)
   record.put("field_g", foo.field_g)
   record.put("field_h", foo.field_h)
   record.put("enum", foo.enum)
   record.put("listOfEnums", foo.listOfEnums)
   record.put("setOfEnums", foo.setOfEnums)
   record.put("listOfLongs", foo.listOfLongs)
   record.put("setOfStrings", foo.setOfStrings)
   record.put("complexList", foo.complexList.map {
      val barRecord = GenericData.Record(barSchema)
      barRecord.put("field_a", it.field_a)
      barRecord.put("field_b", it.field_b)
      barRecord.put("field_c", it.field_c)
      barRecord.put("field_d", it.field_d)
      barRecord
   })
   return record
}

fun deserializeProgramatically(record: GenericRecord): Foo {
   return Foo(
      field_a = record.get("field_a").toString(),
      field_b = record.get("field_b") as Boolean,
      field_c = record.get("field_c") as Int,
      field_d = record.get("field_d") as Double,
      field_e = record.get("field_e") as Int,
      field_f = record.get("field_f").toString(),
      field_g = record.get("field_g").toString(),
      field_h = record.get("field_h") as Int,
      enum = Enum.valueOf((record.get("listOfEnums") as GenericData.EnumSymbol).toString()),
      listOfEnums = (record.get("listOfEnums") as List<GenericData.EnumSymbol>)
         .map { Enum.valueOf(it.toString()) },
      setOfEnums = (record.get("setOfEnums") as List<GenericData.EnumSymbol>)
         .map { Enum.valueOf(it.toString()) }
         .toSet(),
      listOfLongs = record.get("field_i") as List<Long>,
      setOfStrings = (record.get("field_j") as List<String>).toSet(),
      complexList = record.get("field_k") as List<Bar>,
   )
}
