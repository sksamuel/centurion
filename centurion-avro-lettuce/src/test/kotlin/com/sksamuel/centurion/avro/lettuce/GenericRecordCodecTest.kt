package com.sksamuel.centurion.avro.lettuce
//
//import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
//import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
//import io.kotest.core.extensions.install
//import io.kotest.core.spec.style.FunSpec
//import io.kotest.matchers.shouldBe
//import io.lettuce.core.codec.RedisCodec
//import io.lettuce.core.codec.StringCodec
//import kotlinx.coroutines.future.await
//import org.apache.avro.SchemaBuilder
//import org.apache.avro.generic.GenericRecord
//import org.apache.avro.io.DecoderFactory
//import org.apache.avro.io.EncoderFactory
//
//class GenericRecordCodecTest : FunSpec({
//
//   val schema = SchemaBuilder.record("Foo").fields()
//      .optionalString("a")
//      .requiredBoolean("b")
//      .requiredInt("c")
//      .endRecord()
//
//   val valueCodec = GenericRecordCodec(
//      schema,
//      encoderFactory = EncoderFactory.get(),
//      decoderFactory = DecoderFactory.get(),
//   )
//
//   val keyCodec = StringCodec.UTF8
//
//   val redis = install(redisExtension)
//   val conn = redis.toConnection(
//      codec = RedisCodec.of(keyCodec, valueCodec)
//   )
//
//   val encoder = ReflectionRecordEncoder<Foo>(schema)
//   val decoder = ReflectionRecordDecoder<Foo>(schema)
//
//   test("happy path") {
//      val foo = Foo("hello", true, 4)
//      conn.async().set("foo", encoder.encode(schema, foo) as GenericRecord).await()
//      val record = conn.async().get("foo").await()
//      decoder.decode(schema, record) shouldBe foo
//   }
//
//})
//
data class Foo(val a: String?, val b: Boolean, val c: Int)
