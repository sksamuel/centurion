@file:Suppress("RECEIVER_NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")

package com.sksamuel.centurion.avro.decoders

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
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
//         .name("field_i")
//         .type(arraySchema).noDefault()
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
//      val field_i: List<Long>,
   )

   val ids = listOf(
      123123123L,
      123123124,
      123123125,
      123123126,
      123123127,
      123123128,
      123123129,
      123123130,
      123123131,
      123123132,
      123123133,
      123123134
   )

   val avro = Foo::class.java.getResourceAsStream("/benchmark.avro").readAllBytes()

   val json =
      """{"field_a":"hello world","field_b":true,"field_c":123456,"field_d":56.331,"field_e":998876324,"field_f":"stringy mcstring face","field_g":"another string","field_h":821377124}"""

   val sets = 5
   val reps = 10_000_000

   Decoder.useStrictPrimitiveDecoders = true

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

