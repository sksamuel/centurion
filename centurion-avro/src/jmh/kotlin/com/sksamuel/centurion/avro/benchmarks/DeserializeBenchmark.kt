package com.sksamuel.centurion.avro.benchmarks

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryReader
import com.sksamuel.centurion.avro.io.BinaryWriter
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
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
import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit
import kotlin.random.Random

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
   fun deserializeAvroReflectionRecordDecoderNoReuse(blackhole: Blackhole) {
      val reader = BinaryReader(schema, ByteArrayInputStream(avro), df, decoder, null)
      val foo = reader.use { it.read() }
      blackhole.consume(foo)
   }

   @Benchmark
   fun deserializeAvroReflectionRecordDecoderWithReuse(blackhole: Blackhole) {
      val reader = BinaryReader(schema, ByteArrayInputStream(avro), df, decoder, reuse)
      val foo = reader.use { it.read() }
      blackhole.consume(foo)
   }

   @Benchmark
   fun deserializeAvroProgramatically(blackhole: Blackhole) {
      val record = reader.read(null, DecoderFactory.get().binaryDecoder(avro, reuse))
      val foo = Foo(
         field_a = record.get("field_a").toString(),
         field_b = record.get("field_b") as Boolean,
         field_c = record.get("field_c") as Int,
         field_d = record.get("field_d") as Double,
         field_e = record.get("field_e") as Int,
         field_f = record.get("field_f").toString(),
         field_g = record.get("field_g").toString(),
         field_h = record.get("field_h") as Int,
         field_i = record.get("field_i") as List<Long>,
         field_j = (record.get("field_j") as List<String>).toSet(),
         field_k = record.get("field_k") as List<Bar>,
      )
      blackhole.consume(foo)
   }
}

val longArraySchema: Schema = SchemaBuilder.array().items().longType()
val stringArraySchema: Schema = SchemaBuilder.array().items().stringType()

val barSchema: Schema = SchemaBuilder.record("bar").fields()
   .requiredString("field_a")
   .requiredBoolean("field_b")
   .requiredInt("field_c")
   .requiredDouble("field_d")
   .endRecord()

val schema: Schema = SchemaBuilder.record("foo").fields()
   .requiredString("field_a")
   .requiredBoolean("field_b")
   .requiredInt("field_c")
   .requiredDouble("field_d")
   .requiredInt("field_e")
   .requiredString("field_f")
   .requiredString("field_g")
   .requiredInt("field_h")
   .name("field_i").type(longArraySchema).noDefault()
   .name("field_j").type(stringArraySchema).noDefault()
   .name("field_k").type(SchemaBuilder.array().items(barSchema)).noDefault()
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
   val field_i: List<Long>,
   val field_j: Set<String>,
   val field_k: List<Bar>,
)

data class Bar(
   val field_a: String,
   val field_b: Boolean,
   val field_c: Int,
   val field_d: Double
)

fun createFoo(): Foo {
   val foo = Foo(
      field_a = "hello world",
      field_b = true,
      field_c = 123456,
      field_d = 56.331,
      field_e = 998876324,
      field_f = "stringy mcstring face",
      field_g = "another string",
      field_h = 821377124,
      field_i = createIds(),
      field_j = createRandomStrings().toSet(),
      field_k = List(10) {
         Bar(
            field_a = "bar string $it",
            field_b = it % 2 == 0,
            field_c = it * 1000,
            field_d = it.toDouble() / 3.14
         )
      }
   )
   return foo
}

fun createIds(): List<Long> {
   return List(600) { Random.nextLong(0, 750_000_000L) }
}

fun createRandomStrings(): List<String> {
   val charPool = ('a'..'z') + ('A'..'Z') + ('0'..'9')
   return List(100) {
      val length = Random.nextInt(10, 31) // Between 10 and 30 characters
      (1..length)
         .map { Random.nextInt(0, charPool.size) }
         .map(charPool::get)
         .joinToString("")
   }
}

private fun createJson(foo: Foo): ByteArray {
   val mapper = jacksonObjectMapper()
   return mapper.writeValueAsBytes(foo)
}

private fun createAvroBytes(foo: Foo): ByteArray {
   val baos = ByteArrayOutputStream()
   val encoder = ReflectionRecordEncoder<Foo>(schema)
   BinaryWriter(
      schema = schema,
      out = baos,
      ef = EncoderFactory.get(),
      encoder = encoder,
      reuse = null
   ).use { it.write(foo) }

   return baos.toByteArray()
}
