@file:Suppress("RECEIVER_NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS", "PropertyName", "UNCHECKED_CAST")

package com.sksamuel.centurion.avro.benchmarks

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sksamuel.centurion.avro.Foo
import com.sksamuel.centurion.avro.createFoo
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryReader
import com.sksamuel.centurion.avro.io.BinaryWriter
import com.sksamuel.centurion.avro.schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.time.measureTime

fun main() {

   GenericData.get().setFastReaderEnabled(true)
   GenericData.setStringType(schema, GenericData.StringType.String)

   val foo = createFoo()
   val avro = createAvroBytes(foo)
   val json = createJson(foo)

   val sets = 3
   val reps = 100_000

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
      val decoder = ReflectionRecordDecoder<Foo>(schema)
      val time = measureTime {
         repeat(reps) {
            val reader = BinaryReader(schema, ByteArrayInputStream(avro), df, decoder, null)
            val foo = reader.use { it.read() }
            count += foo.field_c
         }
      }
      println("Deserialize Avro (ReflectionRecordDecoder, no reuse):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var count = 0
      val df = DecoderFactory.get()
      val reuse = df.binaryDecoder(emptyArray<Byte>().toByteArray(), null)
      val decoder = ReflectionRecordDecoder<Foo>(schema)
      val time = measureTime {
         repeat(reps) {
            val reader = BinaryReader(schema, ByteArrayInputStream(avro), df, decoder, reuse)
            val foo = reader.use { it.read() }
            count += foo.field_c
         }
      }
      println("Deserialize Avro (ReflectionRecordDecoder, reuse):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var count = 0
      val reuse = DecoderFactory.get().binaryDecoder(emptyArray<Byte>().toByteArray(), null)
      val reader = GenericDatumReader<GenericRecord>(schema)
      val time = measureTime {
         repeat(reps) {
            val record = reader.read(null, DecoderFactory.get().binaryDecoder(avro, reuse))
            val foo = Foo(
               record.get("field_a").toString(),
               record.get("field_b") as Boolean,
               record.get("field_c") as Int,
               record.get("field_d") as Double,
               record.get("field_e") as Int,
               record.get("field_f").toString(),
               record.get("field_g").toString(),
               record.get("field_h") as Int,
               record.get("field_i") as List<Long>,
               (record.get("field_j") as List<String>).toSet(),
            )
            count += foo.field_c
         }
      }
      println("Deserialize Avro (Programatically):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }
}

fun createJson(foo: Foo): ByteArray {
   val mapper = jacksonObjectMapper()
   val json = mapper.writeValueAsBytes(foo)
   return json
}

fun createAvroBytes(foo: Foo): ByteArray {

   val baos = ByteArrayOutputStream()
   val encoder = ReflectionRecordEncoder<Foo>(schema)
   BinaryWriter(
      schema = schema,
      out = baos,
      binaryEncoder = EncoderFactory.get().binaryEncoder(baos, null),
      encoder = encoder,
   ).use { it.write(foo) }

   return baos.toByteArray()
}
