package com.sksamuel.centurion.avro.benchmarks

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.io.BinaryReader
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Fork
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Setup
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import org.openjdk.jmh.infra.Blackhole
import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit

/**
 * JMH benchmark for deserialization performance.
 *
 * To run this benchmark:
 * ./gradlew :centurion-avro:jmh -Pjmh.args="DeserializeBenchmark"
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = ["-Xms2G", "-Xmx2G"])
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
open class DeserializeBenchmark {

   private val df = DecoderFactory.get()
   private val decoder = ReflectionRecordDecoder<Foo>(schema)
   private val mapper = jacksonObjectMapper()

   private val foo = createFoo()
   private val json = createJson(foo)
   private val avro = createAvroBytes(foo)

   private val reuse = df.binaryDecoder(emptyArray<Byte>().toByteArray(), null)
   private val reader = GenericDatumReader<GenericRecord>(schema)

   @Setup
   fun setup() {
      GenericData.get().setFastReaderEnabled(true)
      GenericData.setStringType(schema, GenericData.StringType.String)
   }

   @Benchmark
   fun deserializeJsonJackson(blackhole: Blackhole) {
      val foo = mapper.readValue<Foo>(json)
      blackhole.consume(foo)
   }

   @Benchmark
   fun deserializeAvroReflectionNoReuse(blackhole: Blackhole) {
      val reader = BinaryReader(schema, ByteArrayInputStream(avro), df, decoder, null)
      val foo = reader.use { it.read() }
      blackhole.consume(foo)
   }

   @Benchmark
   fun deserializeAvroReflectionWithReuse(blackhole: Blackhole) {
      val reader = BinaryReader(schema, ByteArrayInputStream(avro), df, decoder, reuse)
      val foo = reader.use { it.read() }
      blackhole.consume(foo)
   }

   @Benchmark
   fun deserializeAvroProgramaticallyNoReuse(blackhole: Blackhole) {
      val record = reader.read(null, DecoderFactory.get().binaryDecoder(avro, null))
      val foo = deserializeProgramatically(record)
      blackhole.consume(foo)
   }
   @Benchmark
   fun deserializeAvroProgramaticallyWithReuse(blackhole: Blackhole) {
      val record = reader.read(null, DecoderFactory.get().binaryDecoder(avro, reuse))
      val foo = deserializeProgramatically(record)
      blackhole.consume(foo)
   }
}

