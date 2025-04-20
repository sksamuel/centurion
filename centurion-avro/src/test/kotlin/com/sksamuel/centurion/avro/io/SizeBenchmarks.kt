package com.sksamuel.centurion.avro.io

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sksamuel.centurion.avro.decoders.schema
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.encoders.reusedEncoder
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

fun main() {

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

   val foo = Foo(
      field_a = "hello world",
      field_b = true,
      field_c = 123456,
      field_d = 56.331,
      field_e = 998876324,
      field_f = "stringy mcstring face",
      field_g = "another string",
      field_h = 821377124,
      field_i = ids,
      field_j = listOf(
         Foo2(1, "hello"),
         Foo2(2, "world"),
      ),
      field_k = ids.map { it.toInt() }.toSet(),
   )

   val objects = 1_000_000

   println("Encoding sizes for $objects objects")

   val writer = GenericDatumWriter<GenericRecord>(schema)
   val encoder = ReflectionRecordEncoder()
   var size = 0
   repeat(objects) {
      size += (encoder.encode(schema, foo) as GenericRecord).reusedEncoder(writer).size
   }
   println("Size Avro:".padEnd(50) + " ${size / 1000000}Mb")

   size = 0
   repeat(objects) {
      val baos = ByteArrayOutputStream()
      val outputStream = GZIPOutputStream(baos)
      outputStream.write((encoder.encode(schema, foo) as GenericRecord).reusedEncoder(writer))
      outputStream.close()
      size += baos.toByteArray().size
   }
   println("Size Avro Gzipped:".padEnd(50) + " ${size / 1000000}Mb")

   val mapper = jacksonObjectMapper()
   size = 0
   repeat(objects) {
      size += mapper.writeValueAsBytes(foo).size
   }
   println("Size Jackson:".padEnd(50) + " ${size / 1000000}Mb")

   size = 0
   repeat(objects) {
      val baos = ByteArrayOutputStream()
      val outputStream = GZIPOutputStream(baos)
      outputStream.write(mapper.writeValueAsBytes(foo))
      outputStream.close()
      size += baos.toByteArray().size
   }
   println("Size Jackson Gzipped:".padEnd(50) + " ${size / 1000000}Mb")

}
