@file:Suppress("RECEIVER_NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")

package com.sksamuel.centurion.avro.decoders

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sksamuel.centurion.avro.encoders.toBinaryByteArrayAvro
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.time.measureTime

val foo2Schema = SchemaBuilder.record("foo2").fields()
   .requiredInt("a")
   .requiredString("b")
   .endRecord()

val arraySchema2 = SchemaBuilder.array().items(foo2Schema)

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
      .endRecord()


fun main() {

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
      val field_k: Set<Int>,
   )

   val avro = Foo::class.java.getResourceAsStream("/benchmark.avro").readAllBytes()

   val json =
      """{"field_a":"hello world","field_b":true,"field_c":123456,"field_d":56.331,"field_e":998876324,"field_f":"stringy mcstring face","field_g":"another string","field_h":821377124,"field_i":[55,66,88,99,77,88,99,66,55,44,33,22,11],"field_j":[{"a":1, "b":"hello"},{"a":2,"b":"world"}],"field_k":[55,66,88,99,77,88,99,66,55,44,33,22,11]}""".toByteArray()

   val sets = 3
   val reps = 4_000_000

   writeAvro()

   repeat(sets) {
      val df = DecoderFactory.get().binaryDecoder(emptyArray<Byte>().toByteArray(), null)
      val record = GenericData.Record(schema)
      val reader = GenericDatumReader<GenericRecord>(schema)
      val decoder = SpecificRecordDecoder<Foo>(Foo::class)
      val time = measureTime {
         repeat(reps) {
            val record = reader.read(record, DecoderFactory.get().binaryDecoder(avro, df))
            decoder.decode(schema, record)
         }
      }
      println("Deserialize Avro bytes (SpecificRecordDecoder):".padEnd(75) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      val df = DecoderFactory.get().binaryDecoder(emptyArray<Byte>().toByteArray(), null)
      val record = GenericData.Record(schema)
      val reader = GenericDatumReader<GenericRecord>(schema)
      val time = measureTime {
         repeat(reps) {
            val record = reader.read(record, DecoderFactory.get().binaryDecoder(avro, df))
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
               (record.get("field_k") as List<Int>).toSet(),
            )
         }
      }
      println("Deserialize Avro bytes (Programatically):".padEnd(75) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      val mapper = jacksonObjectMapper()
      var count = 0
      val time = measureTime {
         repeat(reps) {
            count += mapper.readValue<Foo>(json).field_c
         }
      }
      println("Deserialize Jackson:".padEnd(75) + " ${time.inWholeMilliseconds}ms")
   }
}

fun writeAvro() {

   val foo1 = GenericData.Record(foo2Schema)
   foo1.put("a", 1)
   foo1.put("b", "hello")

   val foo2 = GenericData.Record(foo2Schema)
   foo2.put("a", 2)
   foo2.put("b", "world")

   val record = GenericData.Record(schema)
   record.put("field_a", "hello world")
   record.put("field_b", true)
   record.put("field_c", 123456)
   record.put("field_d", 56.331)
   record.put("field_e", 998876324)
   record.put("field_f", "stringy mcstring face")
   record.put("field_g", "another string")
   record.put("field_h", 821377124)
   record.put("field_i", listOf(55, 66, 88, 99, 77, 88, 99, 66, 55, 44, 33, 22, 11))
   record.put("field_j", listOf(foo1, foo2))
   record.put("field_k", listOf(55, 66, 88, 99, 77, 88, 99, 66, 55, 44, 33, 22, 11))

   Files.write(Paths.get("benchmark.avro"), record.toBinaryByteArrayAvro())
}
