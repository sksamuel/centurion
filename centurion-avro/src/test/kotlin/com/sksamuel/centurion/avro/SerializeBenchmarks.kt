package com.sksamuel.centurion.avro

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sksamuel.centurion.avro.encoders.Encoder
import com.sksamuel.centurion.avro.encoders.MethodHandlesEncoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.encoders.SpecificRecordEncoder
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
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
      return record
   }

   fun createSpecificRecordEncoder() = SpecificRecordEncoder<Foo>().encode(schema)
   fun createReflectionRecordEncoder() = ReflectionRecordEncoder().encode(schema)
   fun createMethodHandlesEncoder() = MethodHandlesEncoder<Foo>().encode(schema)

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
      println("Serialize with Jackson:".padEnd(100) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      val writer = GenericDatumWriter<GenericRecord>(schema)
      val encoder = createMethodHandlesEncoder()
      val time = measureTime {
         repeat(reps) {
            (encoder.invoke(foo) as GenericRecord).reusedEncoder(writer)
         }
      }
      println("Serialize as Avro bytes (MethodHandlesEncoder):".padEnd(100) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      val writer = GenericDatumWriter<GenericRecord>(schema)
      val encoder = createSpecificRecordEncoder()
      val time = measureTime {
         repeat(reps) {
            (encoder.invoke(foo) as GenericRecord).reusedEncoder(writer)
         }
      }
      println("Serialize as Avro bytes (SpecificRecordEncoder):".padEnd(100) + " ${time.inWholeMilliseconds}ms")
   }

//   repeat(sets) {
//      val writer = GenericDatumWriter<GenericRecord>(schema)
//      val encoder = createReflectionRecordEncoder()
//      val time = measureTime {
//         repeat(reps) {
//            (encoder.invoke(foo) as GenericRecord).reusedEncoder(writer)
//         }
//      }
//      println("Serialize as Avro bytes (ReflectionRecordEncoder):".padEnd(100) + " ${time.inWholeMilliseconds}ms")
//   }

   repeat(sets) {
      val writer = GenericDatumWriter<GenericRecord>(schema)
      val time = measureTime {
         repeat(reps) {
            createRecordProgramatically(foo).reusedEncoder(writer)
         }
      }
      println("Serialize as Avro bytes:".padEnd(100) + " ${time.inWholeMilliseconds}ms")
   }

}

//private val encoder = EncoderFactory.get().binaryEncoder(ByteArrayOutputStream(), null)

/**
 * Returns a byte array that contains the avro binary only.
 */
fun GenericRecord.toBinaryByteArrayAvro(): ByteArray {
   val baos = ByteArrayOutputStream()
   val writer = GenericDatumWriter<GenericRecord>(schema)
   val encoder = EncoderFactory.get().binaryEncoder(baos, null)
   writer.write(this, encoder)
   encoder.flush()
   return baos.toByteArray()
}

/**
 * Returns a byte array that contains the avro binary only.
 */
fun GenericRecord.toBinaryByteArrayAvro(writer: DatumWriter<GenericRecord>): ByteArray {
   val baos = ByteArrayOutputStream()
   val encoder = EncoderFactory.get().binaryEncoder(baos, null)
   writer.write(this, encoder)
   encoder.flush()
   return baos.toByteArray()
}

private val encoder = EncoderFactory.get().binaryEncoder(ByteArrayOutputStream(), null)

fun GenericRecord.reusedEncoder(writer: DatumWriter<GenericRecord>): ByteArray {
   val baos = ByteArrayOutputStream()
   val encoder = EncoderFactory.get().binaryEncoder(baos, encoder)
   writer.write(this, encoder)
   encoder.flush()
   return baos.toByteArray()
}

val factory = EncoderFactory().configureBufferSize(512)

/**
 * Returns a byte array that contains the avro binary only.
 */
fun GenericRecord.toByteArray(writer: DatumWriter<GenericRecord>): ByteArray {
   val baos = ByteArrayOutputStream()
   val encoder = factory.binaryEncoder(baos, null)
   writer.write(this, encoder)
   encoder.flush()
   return baos.toByteArray()
}
