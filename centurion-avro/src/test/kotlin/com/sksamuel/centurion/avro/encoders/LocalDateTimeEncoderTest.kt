package com.sksamuel.centurion.avro.encoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.time.LocalDateTime
import java.time.ZoneOffset

class LocalDateTimeEncoderTest : FunSpec({

   val instant = LocalDateTime.of(2026, 5, 12, 10, 20, 30, 123_456_000)

   test("localTimestamp-millis logical type encodes as epoch milli Long at UTC") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.localTimestampMillis().addToSchema(schema)
      LocalDateTimeEncoder.encode(schema, instant) shouldBe instant.toInstant(ZoneOffset.UTC).toEpochMilli()
   }

   test("localTimestamp-micros logical type encodes as epoch micro Long") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.localTimestampMicros().addToSchema(schema)
      val epochSec = instant.toEpochSecond(ZoneOffset.UTC)
      val expected = epochSec * 1_000_000L + instant.nano / 1_000L
      LocalDateTimeEncoder.encode(schema, instant) shouldBe expected
   }

   test("plain LONG with no logical type encodes as epoch millis at UTC") {
      val schema = Schema.create(Schema.Type.LONG)
      LocalDateTimeEncoder.encode(schema, instant) shouldBe instant.toInstant(ZoneOffset.UTC).toEpochMilli()
   }

   test("non-long schemas are rejected") {
      val schema = Schema.create(Schema.Type.INT)
      shouldThrow<IllegalStateException> {
         LocalDateTimeEncoder.encode(schema, instant)
      }
   }
})
