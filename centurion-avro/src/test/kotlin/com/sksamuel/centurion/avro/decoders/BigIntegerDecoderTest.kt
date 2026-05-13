package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.encoders.BigIntegerBytesEncoder
import com.sksamuel.centurion.avro.encoders.BigIntegerStringEncoder
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import java.math.BigInteger
import java.nio.ByteBuffer

class BigIntegerDecoderTest : FunSpec({

   test("BigIntegerBytesDecoder round-trips through BigIntegerBytesEncoder for positives") {
      val schema = Schema.create(Schema.Type.BYTES)
      val input = BigInteger("12345")
      BigIntegerBytesDecoder.decode(schema, BigIntegerBytesEncoder.encode(schema, input)) shouldBe input
   }

   test("BigIntegerBytesDecoder round-trips for negatives") {
      val schema = Schema.create(Schema.Type.BYTES)
      val input = BigInteger("-987654321")
      BigIntegerBytesDecoder.decode(schema, BigIntegerBytesEncoder.encode(schema, input)) shouldBe input
   }

   test("BigIntegerBytesDecoder round-trips for very large values") {
      val schema = Schema.create(Schema.Type.BYTES)
      val input = BigInteger("123456789012345678901234567890")
      BigIntegerBytesDecoder.decode(schema, BigIntegerBytesEncoder.encode(schema, input)) shouldBe input
   }

   test("BigIntegerBytesDecoder accepts a raw ByteArray") {
      val schema = Schema.create(Schema.Type.BYTES)
      val input = BigInteger("12345")
      BigIntegerBytesDecoder.decode(schema, input.toByteArray()) shouldBe input
   }

   test("BigIntegerBytesDecoder honours a positioned ByteBuffer") {
      val schema = Schema.create(Schema.Type.BYTES)
      val input = BigInteger("1234567")
      val raw = input.toByteArray()
      val padded = ByteBuffer.wrap(byteArrayOf(0, 0) + raw)
      padded.position(2)
      BigIntegerBytesDecoder.decode(schema, padded) shouldBe input
   }

   test("BigIntegerBytesDecoder does not mutate the source ByteBuffer position") {
      val schema = Schema.create(Schema.Type.BYTES)
      val raw = BigInteger("42").toByteArray()
      val buffer = ByteBuffer.wrap(raw)
      val before = buffer.position()
      BigIntegerBytesDecoder.decode(schema, buffer)
      buffer.position() shouldBe before
      buffer.remaining() shouldBe raw.size
   }

   test("BigIntegerBytesDecoder rejects an unsupported value type") {
      val schema = Schema.create(Schema.Type.BYTES)
      shouldThrow<IllegalStateException> {
         BigIntegerBytesDecoder.decode(schema, "not bytes")
      }
   }

   test("BigIntegerBytesDecoder rejects a non-BYTES schema") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalArgumentException> {
         BigIntegerBytesDecoder.decode(schema, "1".encodeToByteArray())
      }
   }

   test("BigIntegerStringDecoder decodes a Kotlin String") {
      val schema = Schema.create(Schema.Type.STRING)
      BigIntegerStringDecoder.decode(schema, "12345") shouldBe BigInteger("12345")
   }

   test("BigIntegerStringDecoder decodes a Utf8 (CharSequence)") {
      val schema = Schema.create(Schema.Type.STRING)
      BigIntegerStringDecoder.decode(schema, Utf8("999")) shouldBe BigInteger("999")
   }

   test("BigIntegerStringDecoder round-trips through BigIntegerStringEncoder") {
      val schema = Schema.create(Schema.Type.STRING)
      val input = BigInteger("123456789012345678901234567890")
      BigIntegerStringDecoder.decode(schema, BigIntegerStringEncoder.encode(schema, input)) shouldBe input
   }

   test("BigIntegerStringDecoder rejects a non-string value") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalStateException> {
         BigIntegerStringDecoder.decode(schema, 12345)
      }
   }
})
