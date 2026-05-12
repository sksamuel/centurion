package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.decoders.BigDecimalBytesDecoder
import com.sksamuel.centurion.avro.decoders.BigDecimalFixedDecoder
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeTypeOf
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.math.BigDecimal
import java.nio.ByteBuffer

class BigDecimalEncoderTest : FunSpec({

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

   test("BigDecimalBytesEncoder encodes a BigDecimal as a ByteBuffer") {
      val schema = bytesSchema(10, 2)
      val encoded = BigDecimalBytesEncoder.encode(schema, BigDecimal("123.45"))
      encoded.shouldBeInstanceOf<ByteBuffer>()
      BigDecimalBytesDecoder.decode(schema, encoded) shouldBe BigDecimal("123.45")
   }

   test("BigDecimalBytesEncoder rescales the input to the schema scale (HALF_UP)") {
      val schema = bytesSchema(10, 2)
      val encoded = BigDecimalBytesEncoder.encode(schema, BigDecimal("1.005"))
      BigDecimalBytesDecoder.decode(schema, encoded) shouldBe BigDecimal("1.01")
   }

   test("BigDecimalBytesEncoder requires a BYTES schema") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalArgumentException> {
         BigDecimalBytesEncoder.encode(schema, BigDecimal("1.23"))
      }
   }

   test("BigDecimalStringEncoder encodes a BigDecimal as a Utf8") {
      val schema = Schema.create(Schema.Type.STRING)
      BigDecimalStringEncoder.encode(schema, BigDecimal("123.456")) shouldBe Utf8("123.456")
   }

   test("BigDecimalStringEncoder requires a STRING schema") {
      val schema = Schema.create(Schema.Type.BYTES)
      shouldThrow<IllegalArgumentException> {
         BigDecimalStringEncoder.encode(schema, BigDecimal("1.23"))
      }
   }

   test("BigDecimalFixedEncoder encodes a BigDecimal as a GenericFixed") {
      val schema = fixedSchema(size = 8, precision = 12, scale = 3)
      val encoded = BigDecimalFixedEncoder.encode(schema, BigDecimal("42.500"))
      encoded.shouldBeTypeOf<GenericData.Fixed>()
      BigDecimalFixedDecoder.decode(schema, encoded) shouldBe BigDecimal("42.500")
   }

   test("BigDecimalFixedEncoder rescales the input to the schema scale (HALF_UP)") {
      val schema = fixedSchema(size = 8, precision = 12, scale = 2)
      val encoded = BigDecimalFixedEncoder.encode(schema, BigDecimal("2.345"))
      BigDecimalFixedDecoder.decode(schema, encoded) shouldBe BigDecimal("2.35")
   }

   test("BigDecimalFixedEncoder requires a FIXED schema") {
      val schema = Schema.create(Schema.Type.BYTES)
      shouldThrow<IllegalArgumentException> {
         BigDecimalFixedEncoder.encode(schema, BigDecimal("1.23"))
      }
   }
})
