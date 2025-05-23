package com.sksamuel.centurion.avro.io

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sksamuel.centurion.avro.decoders.Foo
import com.sksamuel.centurion.avro.decoders.Foo2
import com.sksamuel.centurion.avro.decoders.schema
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import org.apache.avro.generic.GenericData
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.util.zip.DeflaterOutputStream
import java.util.zip.GZIPOutputStream
import kotlin.random.Random

fun main() {
   GenericData.setStringType(schema, GenericData.StringType.String)

   val ids = List(600) { Random.nextLong(1, 750_000_000) }

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
      field_k = ids.map { it.toInt() },
      field_l = setOf("hello", "world"),
   )

   val encoder = ReflectionRecordEncoder<Foo>(schema)

   val baos = ByteArrayOutputStream()
   val writer1 = BinaryWriter(schema, baos, EncoderFactory.get().binaryEncoder(baos, null), encoder)
   writer1.write(foo)
   writer1.close()
   var size = baos.toByteArray().size
   println("Size Avro:".padEnd(50) + " ${size}b")

   val baos2 = ByteArrayOutputStream()
   val output2 = GZIPOutputStream(baos2)
   val writer2 = BinaryWriter(schema, output2, EncoderFactory.get().binaryEncoder(baos2, null), encoder)
   writer2.write(foo)
   writer2.close()
   size = baos2.toByteArray().size
   println("Size Avro GZIPOutputStream:".padEnd(50) + " ${size}b")

   val baos4 = ByteArrayOutputStream()
   val output4 = DeflaterOutputStream(baos4)
   val writer4 = BinaryWriter(schema, output4, EncoderFactory.get().binaryEncoder(baos4, null), encoder)
   writer4.write(foo)
   writer4.close()
   size = baos4.toByteArray().size
   println("Size Avro DeflaterOutputStream:".padEnd(50) + " ${size}b")

   val mapper = jacksonObjectMapper()
   size = mapper.writeValueAsBytes(foo).size
   println("Size Jackson:".padEnd(50) + " ${size}b")

   val baos3 = ByteArrayOutputStream()
   val output3 = GZIPOutputStream(baos3)
   output3.write(mapper.writeValueAsBytes(foo))
   output3.close()
   size = baos3.toByteArray().size
   println("Size Jackson GZIPOutputStream:".padEnd(50) + " ${size}b")

}
