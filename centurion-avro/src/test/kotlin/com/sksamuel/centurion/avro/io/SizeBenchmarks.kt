package com.sksamuel.centurion.avro.io

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sksamuel.centurion.avro.decoders.schema
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import org.apache.avro.io.EncoderFactory
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

   val baos = ByteArrayOutputStream()
   val writer1 = BinaryWriter(schema, baos, ReflectionRecordEncoder(), EncoderFactory.get(), null)
   writer1.write(foo)
   writer1.close()
   var size = baos.toByteArray().size
   println("Size Avro:".padEnd(50) + " ${size}b")

   val baos2 = ByteArrayOutputStream()
   val output2 = GZIPOutputStream(baos2)
   val writer2 = BinaryWriter(schema, output2, ReflectionRecordEncoder(), EncoderFactory.get(), null)
   writer2.write(foo)
   writer2.close()
   size = baos2.toByteArray().size
   println("Size Avro GZIPOutputStream:".padEnd(50) + " ${size}b")

   val mapper = jacksonObjectMapper()
   size = mapper.writeValueAsBytes(foo).size
   println("Size Jackson:".padEnd(50) + " ${size}b")

   val baos3 = ByteArrayOutputStream()
   val output3 = GZIPOutputStream(baos3)
   output3.write(mapper.writeValueAsBytes(foo))
   output3.close()
   size = baos3.toByteArray().size
   println("Size Jackson Gzipped:".padEnd(50) + " ${size}b")

}
