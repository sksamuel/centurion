@file:Suppress("RECEIVER_NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")

package com.sksamuel.centurion.avro.decoders

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
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

fun main() {

   val arraySchema = SchemaBuilder.array().items().longType()
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
         .name("field_i").type(arraySchema).noDefault()
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
   )

   val avro = Foo::class.java.getResourceAsStream("/benchmark.avro").readAllBytes()

   val json =
      """{"field_a":"hello world","field_b":true,"field_c":123456,"field_d":56.331,"field_e":998876324,"field_f":"stringy mcstring face","field_g":"another string","field_h":821377124,"field_i":[55,66,88,99,77,88,99,66,55,44,33,22,11]}""".toByteArray()

   val sets = 5
   val reps = 10_000_000

   writeAvro(schema)

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
      println("Deserialize Avro bytes (SpecificRecordDecoder):".padEnd(100) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      val df = DecoderFactory.get().binaryDecoder(emptyArray<Byte>().toByteArray(), null)
      val record = GenericData.Record(schema)
      val reader = GenericDatumReader<GenericRecord>(schema)
      val time = measureTime {
         repeat(reps) {
            val record = reader.read(record, DecoderFactory.get().binaryDecoder(avro, df))
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
            )
         }
      }
      println("Deserialize Avro bytes (Programatically):".padEnd(100) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      val mapper = jacksonObjectMapper()
      var size = 0
      val time = measureTime {
         repeat(reps) {
            size += mapper.readTree(json).size()
         }
      }
      println("Deserialize Jackson:".padEnd(100) + " ${time.inWholeMilliseconds}ms")
   }
}

fun writeAvro(schema: Schema) {
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
   Files.write(Paths.get("benchmark.avro"), record.toBinaryByteArrayAvro())
}
