package com.sksamuel.centurion.avro.encoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import java.math.BigInteger
import java.nio.ByteBuffer

class BigIntegerEncoderTest : FunSpec({

   test("BigIntegerBytesEncoder encodes a positive integer as two's-complement BYTES") {
      val schema = Schema.create(Schema.Type.BYTES)
      val encoded = BigIntegerBytesEncoder.encode(schema, BigInteger("12345")) as ByteBuffer
      val out = ByteArray(encoded.remaining()).also { encoded.duplicate().get(it) }
      out shouldBe BigInteger("12345").toByteArray()
   }

   test("BigIntegerBytesEncoder encodes a negative integer as two's-complement BYTES") {
      val schema = Schema.create(Schema.Type.BYTES)
      val encoded = BigIntegerBytesEncoder.encode(schema, BigInteger("-1")) as ByteBuffer
      val out = ByteArray(encoded.remaining()).also { encoded.duplicate().get(it) }
      out shouldBe byteArrayOf(-1)
   }

   test("BigIntegerBytesEncoder encodes a large value that does not fit in a Long") {
      val schema = Schema.create(Schema.Type.BYTES)
      val huge = BigInteger("123456789012345678901234567890")
      val encoded = BigIntegerBytesEncoder.encode(schema, huge) as ByteBuffer
      val out = ByteArray(encoded.remaining()).also { encoded.duplicate().get(it) }
      out shouldBe huge.toByteArray()
   }

   test("BigIntegerBytesEncoder rejects a non-BYTES schema") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalArgumentException> {
         BigIntegerBytesEncoder.encode(schema, BigInteger.ONE)
      }
   }

   test("BigIntegerStringEncoder encodes as a Utf8 String") {
      val schema = Schema.create(Schema.Type.STRING)
      BigIntegerStringEncoder.encode(schema, BigInteger("12345")) shouldBe Utf8("12345")
   }

   test("BigIntegerStringEncoder rejects a non-STRING schema") {
      val schema = Schema.create(Schema.Type.BYTES)
      shouldThrow<IllegalArgumentException> {
         BigIntegerStringEncoder.encode(schema, BigInteger.ONE)
      }
   }
})
