package com.sksamuel.centurion.avro.encoders

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sksamuel.centurion.avro.io.BinaryWriter
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import kotlin.time.measureTime

fun main() {

   Encoder.globalUseJavaString = true

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
//      val field_j: Set<Long>,
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

   val foo = Foo(
      field_a = "hello world",
      field_b = true,
      field_c = 123456,
      field_d = 56.331,
      field_e = 998876324,
      field_f = "stringy mcstring face",
      field_g = "another string",
      field_h = 821377124,
//      field_i = ids,
//      field_j = ids.toSet()
   )

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
//      record.put("field_i", foo.field_i)
//      record.put("field_j", foo.field_j)
      return record
   }

   val sets = 5
   val reps = 30_000_000

   repeat(sets) {
      val mapper = jacksonObjectMapper()
      var size = 0
      val time = measureTime {
         repeat(reps) {
            size += mapper.writeValueAsBytes(foo).size
         }
      }
      println("Serialize as Json (Jackson):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var size = 0
      val encoder = ReflectionRecordEncoder()
      val time = measureTime {
         repeat(reps) {
            val baos = ByteArrayOutputStream()
            val writer = BinaryWriter(schema, baos, encoder, EncoderFactory.get(), null)
            writer.use { it.write(foo) }
            size += baos.toByteArray().size
         }
      }
      println("Serialize as Avro bytes (BinaryWriter):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var size = 0
      val reuse = EncoderFactory.get().binaryEncoder(ByteArrayOutputStream(), null)
      val encoder = ReflectionRecordEncoder()
      val time = measureTime {
         repeat(reps) {
            val baos = ByteArrayOutputStream()
            val writer = BinaryWriter(schema, baos, encoder, EncoderFactory.get(), reuse)
            writer.use { it.write(foo) }
            size += baos.toByteArray().size
         }
      }
      println("Serialize as Avro bytes (BinaryWriter;reused encoder):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var size = 0
      val writer = GenericDatumWriter<GenericRecord>(schema)
      val time = measureTime {
         repeat(reps) {
            val baos = ByteArrayOutputStream()
            val encoder = EncoderFactory.get().binaryEncoder(baos, null)
            val record = createRecordProgramatically(foo)
            writer.write(record, encoder)
            encoder.flush()
            size += baos.toByteArray().size
         }
      }
      println("Serialize as Avro bytes (Programatically):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

}
