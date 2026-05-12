package com.sksamuel.centurion.avro.encoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.time.OffsetDateTime
import java.time.ZoneOffset

class OffsetDateTimeEncoderTest : FunSpec({

   val odt: OffsetDateTime = OffsetDateTime.of(2026, 5, 12, 10, 20, 30, 123_000_000, ZoneOffset.UTC)

   test("timestamp-millis logical type encodes as epoch millis") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMillis().addToSchema(schema)
      OffsetDateTimeEncoder.encode(schema, odt) shouldBe odt.toInstant().toEpochMilli()
   }

   test("timestamp-micros logical type encodes as epoch micros") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMicros().addToSchema(schema)
      val expected = odt.toEpochSecond() * 1_000_000L + odt.nano / 1_000L
      OffsetDateTimeEncoder.encode(schema, odt) shouldBe expected
   }

   test("plain LONG with no logical type encodes as epoch millis") {
      val schema = Schema.create(Schema.Type.LONG)
      OffsetDateTimeEncoder.encode(schema, odt) shouldBe odt.toInstant().toEpochMilli()
   }

   test("encoding normalises the offset to UTC instant") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMillis().addToSchema(schema)
      val utc = OffsetDateTime.of(2026, 5, 12, 12, 0, 0, 0, ZoneOffset.UTC)
      val plusTwo = OffsetDateTime.of(2026, 5, 12, 14, 0, 0, 0, ZoneOffset.ofHours(2))
      OffsetDateTimeEncoder.encode(schema, utc) shouldBe OffsetDateTimeEncoder.encode(schema, plusTwo)
   }

   test("non-long schemas are rejected") {
      val schema = Schema.create(Schema.Type.INT)
      shouldThrow<IllegalStateException> {
         OffsetDateTimeEncoder.encode(schema, odt)
      }
   }
})
