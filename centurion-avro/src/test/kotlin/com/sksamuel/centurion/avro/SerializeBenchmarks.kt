package com.sksamuel.centurion.avro

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryEncoderPool
import com.sksamuel.centurion.avro.io.BinaryWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import kotlin.time.measureTime

fun main() {

   val sets = 3
   val reps = 100_000
   val foo = createFoo()

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
      val mapper = jacksonObjectMapper()
      var size = 0
      val time = measureTime {
         repeat(reps) {
            val baos = ByteArrayOutputStream()
            val output = GZIPOutputStream(baos)
            output.write(mapper.writeValueAsBytes(foo).size)
            output.close()
            size += baos.toByteArray().size
         }
      }
      println("Serialize as Json (Jackson;Gzip):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var size = 0
      val encoder = ReflectionRecordEncoder.Companion<Foo>(schema)
      val time = measureTime {
         repeat(reps) {
            val baos = ByteArrayOutputStream()
            val writer = BinaryWriter(schema, baos, EncoderFactory.get().binaryEncoder(baos, null), encoder)
            writer.use { it.write(foo) }
            size += baos.toByteArray().size
         }
      }
      println("Serialize as Avro bytes (BinaryWriter;no reuse)".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var size = 0
      val encoder = ReflectionRecordEncoder<Foo>(schema)
      val pool = BinaryEncoderPool(Int.MAX_VALUE, EncoderFactory.get())
      val time = measureTime {
         repeat(reps) {
            val baos = ByteArrayOutputStream()
            pool.use(baos) { binaryEncoder ->
               val writer = BinaryWriter(schema, baos, binaryEncoder, encoder)
               writer.use { it.write(foo) }
            }
            size += baos.toByteArray().size
         }
      }
      println("Serialize as Avro bytes (BinaryWriter;pool):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var size = 0
      val encoder = ReflectionRecordEncoder<Foo>(schema)
      val reuse = EncoderFactory.get().binaryEncoder(ByteArrayOutputStream(), null)
      val time = measureTime {
         repeat(reps) {
            val baos = ByteArrayOutputStream()
            val binaryEncoder = EncoderFactory.get().binaryEncoder(baos, reuse)
            val writer = BinaryWriter(schema, baos, binaryEncoder, encoder)
            writer.use { it.write(foo) }
            binaryEncoder.flush()
            size += baos.toByteArray().size
         }
      }
      println("Serialize as Avro bytes (BinaryWriter;reuse):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var size = 0
      val writer = GenericDatumWriter<GenericRecord>(schema)
      val reuse = EncoderFactory.get().binaryEncoder(ByteArrayOutputStream(), null)
      val time = measureTime {
         repeat(reps) {
            val baos = ByteArrayOutputStream()
            val encoder = EncoderFactory.get().binaryEncoder(baos, reuse)
            val record = createRecordProgramatically(foo)
            writer.write(record, encoder)
            encoder.flush()
            size += baos.toByteArray().size
         }
      }
      println("Serialize as Avro bytes (Programatically;reuse encoder):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }

   repeat(sets) {
      var size = 0
      val writer = GenericDatumWriter<GenericRecord>(schema)
      val reuse = EncoderFactory.get().binaryEncoder(ByteArrayOutputStream(), null)
      val time = measureTime {
         repeat(reps) {
            val baos = ByteArrayOutputStream()
            val gzip = GZIPOutputStream(baos)
            val encoder = EncoderFactory.get().binaryEncoder(gzip, reuse)
            val record = createRecordProgramatically(foo)
            writer.write(record, encoder)
            encoder.flush()
            gzip.close()
            size += baos.toByteArray().size
         }
      }
      println("Serialize as Avro bytes (Gzip, Programatically;reuse encoder):".padEnd(60) + " ${time.inWholeMilliseconds}ms")
   }
}
