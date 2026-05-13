package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.decoders.UTF8Decoder
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class Utf8EncoderTest : FunSpec({

   test("STRING schema returns the Utf8 unchanged") {
      val schema = Schema.create(Schema.Type.STRING)
      val input = Utf8("hello")
      Utf8Encoder.encode(schema, input) shouldBe input
   }

   test("BYTES schema wraps the bytes in a fresh ByteBuffer") {
      val schema = Schema.create(Schema.Type.BYTES)
      val encoded = Utf8Encoder.encode(schema, Utf8("hi")) as ByteBuffer
      val out = ByteArray(encoded.remaining()).also { encoded.duplicate().get(it) }
      out shouldBe "hi".encodeToByteArray()
   }

   test("BYTES schema copies (does not share the Utf8 backing array)") {
      val schema = Schema.create(Schema.Type.BYTES)
      val input = Utf8("abc")
      val encoded = Utf8Encoder.encode(schema, input) as ByteBuffer
      // mutating the source should not affect the encoded buffer
      input.bytes[0] = 'Z'.code.toByte()
      val out = ByteArray(encoded.remaining()).also { encoded.duplicate().get(it) }
      out shouldBe "abc".encodeToByteArray()
   }

   test("FIXED schema returns a GenericFixed of the right size, zero-padded") {
      val schema: Schema = SchemaBuilder.builder().fixed("f").size(5)
      val encoded = Utf8Encoder.encode(schema, Utf8("hi")) as GenericData.Fixed
      encoded.bytes() shouldBe byteArrayOf('h'.code.toByte(), 'i'.code.toByte(), 0, 0, 0)
   }

   test("FIXED rejects oversize input with a useful message") {
      val schema: Schema = SchemaBuilder.builder().fixed("f").size(2)
      val ex = shouldThrow<IllegalStateException> {
         Utf8Encoder.encode(schema, Utf8("abcd"))
      }
      ex.message!! shouldContain "4"
      ex.message!! shouldContain "2"
   }

   test("rejects unsupported schema types") {
      val schema = Schema.create(Schema.Type.INT)
      shouldThrow<IllegalStateException> {
         Utf8Encoder.encode(schema, Utf8("x"))
      }
   }

   test("round-trips through UTF8Decoder on a STRING schema") {
      val schema = Schema.create(Schema.Type.STRING)
      val input = Utf8("round-trip me")
      UTF8Decoder.decode(schema, Utf8Encoder.encode(schema, input)) shouldBe input
   }

   test("round-trips through UTF8Decoder on a BYTES schema") {
      val schema = Schema.create(Schema.Type.BYTES)
      val input = Utf8("bytes path")
      UTF8Decoder.decode(schema, Utf8Encoder.encode(schema, input)) shouldBe input
   }

   test("encoderFor returns Utf8Encoder for a Utf8 field") {
      val schema = Schema.create(Schema.Type.STRING)
      Encoder.encoderFor(::sample.returnType, schema) shouldBe Utf8Encoder
   }
})

private val sample: Utf8 get() = Utf8("")
