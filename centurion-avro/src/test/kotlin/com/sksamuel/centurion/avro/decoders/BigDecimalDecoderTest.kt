package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.encoders.BigDecimalBytesEncoder
import com.sksamuel.centurion.avro.encoders.BigDecimalFixedEncoder
import com.sksamuel.centurion.avro.encoders.BigDecimalStringEncoder
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.util.Utf8
import java.math.BigDecimal
import java.nio.ByteBuffer

class BigDecimalDecoderTest : FunSpec({

   fun bytesSchema(precision: Int, scale: Int): Schema {
      val schema = Schema.create(Schema.Type.BYTES)
      LogicalTypes.decimal(precision, scale).addToSchema(schema)
      return schema
   }

   fun fixedSchema(size: Int, precision: Int, scale: Int): Schema {
      val schema = SchemaBuilder.builder().fixed("d").size(size)
      LogicalTypes.decimal(precision, scale).addToSchema(schema)
      return schema
   }

   test("BigDecimalBytesDecoder round-trips through BigDecimalBytesEncoder") {
      val schema = bytesSchema(10, 2)
      val input = BigDecimal("123.45")
      BigDecimalBytesDecoder.decode(schema, BigDecimalBytesEncoder.encode(schema, input)) shouldBe input
   }

   test("BigDecimalBytesDecoder accepts a raw ByteArray") {
      val schema = bytesSchema(10, 2)
      val input = BigDecimal("9.99")
      val encoded = BigDecimalBytesEncoder.encode(schema, input) as ByteBuffer
      val asArray = ByteArray(encoded.remaining()).also { encoded.duplicate().get(it) }
      BigDecimalBytesDecoder.decode(schema, asArray) shouldBe input
   }

   test("BigDecimalBytesDecoder honours a positioned ByteBuffer") {
      val schema = bytesSchema(10, 2)
      val input = BigDecimal("1.50")
      val encoded = BigDecimalBytesEncoder.encode(schema, input) as ByteBuffer
      val bytes = ByteArray(encoded.remaining()).also { encoded.duplicate().get(it) }
      val padded = ByteBuffer.wrap(byteArrayOf(0, 0) + bytes)
      padded.position(2)
      BigDecimalBytesDecoder.decode(schema, padded) shouldBe input
   }

   test("BigDecimalBytesDecoder rejects an unsupported value type") {
      val schema = bytesSchema(10, 2)
      shouldThrow<IllegalStateException> {
         BigDecimalBytesDecoder.decode(schema, "not bytes")
      }
   }

   test("BigDecimalStringDecoder decodes a Kotlin String") {
      val schema = Schema.create(Schema.Type.STRING)
      BigDecimalStringDecoder.decode(schema, "123.456") shouldBe BigDecimal("123.456")
   }

   test("BigDecimalStringDecoder decodes a Utf8") {
      val schema = Schema.create(Schema.Type.STRING)
      BigDecimalStringDecoder.decode(schema, Utf8("0.01")) shouldBe BigDecimal("0.01")
   }

   test("BigDecimalStringDecoder round-trips through BigDecimalStringEncoder") {
      val schema = Schema.create(Schema.Type.STRING)
      val input = BigDecimal("9876543210.123456789")
      BigDecimalStringDecoder.decode(schema, BigDecimalStringEncoder.encode(schema, input)) shouldBe input
   }

   test("BigDecimalStringDecoder rejects a non-string value") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalStateException> {
         BigDecimalStringDecoder.decode(schema, 123)
      }
   }

   test("BigDecimalFixedDecoder round-trips through BigDecimalFixedEncoder") {
      val schema = fixedSchema(size = 8, precision = 12, scale = 3)
      val input = BigDecimal("42.500")
      BigDecimalFixedDecoder.decode(schema, BigDecimalFixedEncoder.encode(schema, input)) shouldBe input
   }

   test("BigDecimalFixedDecoder rejects a non-GenericFixed value") {
      val schema = fixedSchema(size = 8, precision = 12, scale = 3)
      shouldThrow<IllegalStateException> {
         BigDecimalFixedDecoder.decode(schema, byteArrayOf(0, 0, 0, 0, 0, 0, 0, 1))
      }
   }
})
