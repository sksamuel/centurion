@file:Suppress("RECEIVER_NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS", "PropertyName", "UNCHECKED_CAST")

package com.sksamuel.centurion.avro.decoders

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryReader
import com.sksamuel.centurion.avro.io.BinaryWriter
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.time.measureTime

private val foo2Schema = SchemaBuilder.record("foo2").fields()
   .requiredInt("a")
   .requiredString("b")
   .endRecord()

private val arraySchema2 = SchemaBuilder.array().items(foo2Schema)

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
      .name("field_i").type(SchemaBuilder.array().items().longType()).noDefault()
      .name("field_j").type(arraySchema2).noDefault()
      .name("field_k").type(SchemaBuilder.array().items().intType()).noDefault()
      .name("field_l").type(SchemaBuilder.array().items().stringType()).noDefault()
      .endRecord()

data class Foo2(
   val a: Int,
   val b: String,
)

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
   val field_j: List<Foo2>,
   val field_k: List<Int>,
   val field_l: Set<String>,
)

fun main() {

   GenericData.get().setFastReaderEnabled(true)
   GenericData.setStringType(schema, GenericData.StringType.String)

   val avro = createAvroBytes()
   val json = createJson()

   val sets = 3
   val reps = 1_000_000

   createAvroBytes()

   repeat(sets) {
      val mapper = jacksonObjectMapper()
      var count = 0
      val time = measureTime {
         repeat(reps) {
            count += mapper.readValue<Foo>(json).field_c
         }
      }
      println("Deserialize Json (Jackson):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var count = 0
      val df = DecoderFactory.get()
      val reuse = df.binaryDecoder(emptyArray<Byte>().toByteArray(), null)
      val decoder = ReflectionRecordDecoder<Foo>()
      val time = measureTime {
         repeat(reps) {
            val reader = BinaryReader(schema, ByteArrayInputStream(avro), df, decoder, reuse)
            val foo = reader.use { it.read() }
            count += foo.field_c
         }
      }
      println("Deserialize Avro (ReflectionRecordDecoder):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var count = 0
      val reuse = DecoderFactory.get().binaryDecoder(emptyArray<Byte>().toByteArray(), null)
      val reader = GenericDatumReader<GenericRecord>(schema)
      val time = measureTime {
         repeat(reps) {
            val record = reader.read(null, DecoderFactory.get().binaryDecoder(avro, reuse))
            val js = record.get("field_j") as List<GenericData.Record>
            val js2 = js.map { Foo2(it.get("a") as Int, it.get("b").toString()) }
            Foo(
               record.get("field_a").toString(),
               record.get("field_b") as Boolean,
               record.get("field_c") as Int,
               record.get("field_d") as Double,
               record.get("field_e") as Int,
               record.get("field_f").toString(),
               record.get("field_g").toString(),
               record.get("field_h") as Int,
               record.get("field_i") as List<Long>,
               js2,
               (record.get("field_k") as List<Int>),
               (record.get("field_l") as List<String>).toSet(),
            )
            count += foo.field_c
         }
      }
      println("Deserialize Avro (Programatically):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }
}

val foo1 = Foo2(
   a = 1,
   b = "hello",
)

val foo2 = Foo2(
   a = 2,
   b = "world",
)

val ids = (1..100).toList()

val foo = Foo(
   field_a = "hello world",
   field_b = true,
   field_c = 123456,
   field_d = 56.331,
   field_e = 998876324,
   field_f = "stringy mcstring face",
   field_g = "another string",
   field_h = 821377124,
   field_i = ids.map { it.toLong() },
   field_j = listOf(foo1, foo2),
   field_k = ids,
   field_l = ids.map { it.toString() }.toSet(),
)

fun createJson(): ByteArray {
   val mapper = jacksonObjectMapper()
   val json = mapper.writeValueAsBytes(foo)
   return json
}

fun createAvroBytes(): ByteArray {

   val baos = ByteArrayOutputStream()
   BinaryWriter(
      schema = schema,
      output = baos,
      encoder = ReflectionRecordEncoder<Foo>(),
      factory = EncoderFactory.get(),
      reuse = null
   ).use { it.write(foo) }

   return baos.toByteArray()
}
