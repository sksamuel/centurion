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
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
open class SerializeBenchmark {

   private val ef = EncoderFactory.get()
   private val encoder = ReflectionRecordEncoder<Foo>(schema)
   private val mapper = jacksonObjectMapper()

   private val foo = createFoo()

   private val reuse = ef.binaryEncoder(ByteArrayOutputStream(), null)
   private val writer = GenericDatumWriter<GenericRecord>(schema)

   init {
      GenericData.setStringType(schema, GenericData.StringType.String)
   }

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
   fun serializeAvroReflectionNoReuse(blackhole: Blackhole) {
      val baos = ByteArrayOutputStream()
      BinaryWriter(schema, baos, EncoderFactory.get(), encoder, null).use { it.write(foo) }
      blackhole.consume(baos.toByteArray())
   }

   @Benchmark
   fun serializeAvroReflectionWithReuse(blackhole: Blackhole) {
      val baos = ByteArrayOutputStream()
      BinaryWriter(schema, baos, EncoderFactory.get(), encoder, reuse).use { it.write(foo) }
      blackhole.consume(baos.toByteArray())
   }

   @Benchmark
   fun serializeAvroProgramaticallyNoReuse(blackhole: Blackhole) {
      val baos = ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(baos, null)
      val record = createRecordProgramatically(foo)
      writer.write(record, encoder)
      encoder.flush()
      blackhole.consume(baos.toByteArray())
   }

   @Benchmark
   fun serializeAvroProgramaticallyWithReuse(blackhole: Blackhole) {
      val baos = ByteArrayOutputStream()
      val encoder = EncoderFactory.get().binaryEncoder(baos, reuse)
      val record = createRecordProgramatically(foo)
      writer.write(record, encoder)
      encoder.flush()
      blackhole.consume(baos.toByteArray())
   }

   @Benchmark
   fun serializeAvroProgramaticallyGzipNoReuse(blackhole: Blackhole) {
      val baos = ByteArrayOutputStream()
      val gzip = GZIPOutputStream(baos)
      val encoder = EncoderFactory.get().binaryEncoder(gzip, null)
      val record = createRecordProgramatically(foo)
      writer.write(record, encoder)
      encoder.flush()
      gzip.close()
      blackhole.consume(baos.toByteArray())
   }

   @Benchmark
   fun serializeAvroProgramaticallyGzipWithReuse(blackhole: Blackhole) {
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


