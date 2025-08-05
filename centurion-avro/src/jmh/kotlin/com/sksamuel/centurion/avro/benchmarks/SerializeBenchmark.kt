package com.sksamuel.centurion.avro.benchmarks

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import org.openjdk.jmh.infra.Blackhole
import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPOutputStream

/**
 * JMH benchmark for serialization performance.
 *
 * To run this benchmark:
 * ./gradlew :centurion-avro:jmh -Pjmh.args="SerializeBenchmark"
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = ["-Xms2G", "-Xmx2G"])
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
open class SerializeBenchmark {

   private val ef = EncoderFactory.get()
   private val encoder = ReflectionRecordEncoder<Foo>(schema)
   private val mapper = jacksonObjectMapper()

   private val foo = createFoo()

   private val reuse = ef.binaryEncoder(ByteArrayOutputStream(), null)
   private val writer = GenericDatumWriter<GenericRecord>(schema)

   @Benchmark
   fun serializeAsJsonJackson(blackhole: Blackhole) {
      val bytes = mapper.writeValueAsBytes(foo)
      blackhole.consume(bytes)
   }

   @Benchmark
   fun serializeAsJsonJacksonGzip(blackhole: Blackhole) {
      val baos = ByteArrayOutputStream()
      val output = GZIPOutputStream(baos)
      output.write(mapper.writeValueAsBytes(foo))
      output.close()
      blackhole.consume(baos.toByteArray())
   }

   @Benchmark
   fun serializeAsAvroBytesNoReuse(blackhole: Blackhole) {
      val baos = ByteArrayOutputStream()
      val writer = BinaryWriter(schema, baos, EncoderFactory.get(), encoder)
      writer.use { it.write(foo) }
      blackhole.consume(baos.toByteArray())
   }

   @Benchmark
   fun serializeAsAvroBytesWithProgramaticallyReuseEncoder(blackhole: Blackhole) {
      val baos = ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(baos, reuse)
      val record = createRecordProgramatically(foo)
      writer.write(record, encoder)
      encoder.flush()
      blackhole.consume(baos.toByteArray())
   }

   @Benchmark
   fun serializeAsAvroBytesWithGzipProgramaticallyReuseEncoder(blackhole: Blackhole) {
      val baos = ByteArrayOutputStream()
      val gzip = GZIPOutputStream(baos)
      val encoder = EncoderFactory.get().binaryEncoder(gzip, reuse)
      val record = createRecordProgramatically(foo)
      writer.write(record, encoder)
      encoder.flush()
      gzip.close()
      blackhole.consume(baos.toByteArray())
   }
}

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
   record.put("field_i", foo.field_i)
   record.put("field_j", foo.field_j)
   record.put("field_k", foo.field_k.map {
      val barRecord = GenericData.Record(barSchema)
      barRecord.put("field_a", it.field_a)
      barRecord.put("field_b", it.field_b)
      barRecord.put("field_c", it.field_c)
      barRecord.put("field_d", it.field_d)
      barRecord
   })
   return record
}

