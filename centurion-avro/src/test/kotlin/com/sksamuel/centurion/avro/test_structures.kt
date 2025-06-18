package com.sksamuel.centurion.avro

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import kotlin.random.Random

val longArraySchema: Schema = SchemaBuilder.array().items().longType()
val stringArraySchema: Schema = SchemaBuilder.array().items().stringType()

val barSchema: Schema = SchemaBuilder.record("foo").fields()
   .requiredString("field_a")
   .requiredBoolean("field_b")
   .requiredInt("field_c")
   .requiredDouble("field_d")
   .endRecord()

val schema: Schema =
   SchemaBuilder.record("foo").fields()
      .requiredString("field_a")
      .requiredBoolean("field_b")
      .requiredInt("field_c")
      .requiredDouble("field_d")
      .requiredInt("field_e")
      .requiredString("field_f")
      .requiredString("field_g")
      .requiredInt("field_h")
      .name("field_i").type(longArraySchema).noDefault()
      .name("field_j").type(stringArraySchema).noDefault()
      .name("field_k").type(SchemaBuilder.array().items(barSchema)).noDefault()
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
   val field_i: List<Long>,
   val field_j: Set<String>,
   val field_k: List<Bar>,
)

data class Bar(
   val field_a: String,
   val field_b: Boolean,
   val field_c: Int,
   val field_d: Double
)

fun createIds(): List<Long> {
   return List(600) { Random.nextLong(0, 750_000_000L) }
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
      field_i = createIds(),
      field_j = createRandomStrings().toSet(),
      field_k = List(10) {
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
   record.put("field_i", foo.field_i)
   record.put("field_j", foo.field_j)
   return record
}
