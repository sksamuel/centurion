package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.encoders.OffsetDateTimeEncoder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

class OffsetDateTimeDecoderTest : FunSpec({

   test("round-trips through OffsetDateTimeEncoder with timestamp-millis") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMillis().addToSchema(schema)
      val input = Instant.ofEpochMilli(1_700_000_000_000).atOffset(ZoneOffset.UTC)
      OffsetDateTimeDecoder.decode(schema, OffsetDateTimeEncoder.encode(schema, input)) shouldBe input
   }

   test("round-trips through OffsetDateTimeEncoder with timestamp-micros") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMicros().addToSchema(schema)
      val input = Instant.ofEpochMilli(1_700_000_000_000).atOffset(ZoneOffset.UTC)
      OffsetDateTimeDecoder.decode(schema, OffsetDateTimeEncoder.encode(schema, input)) shouldBe input
   }

   test("long without a logical type yields a UTC OffsetDateTime") {
      val schema = Schema.create(Schema.Type.LONG)
      val input = OffsetDateTime.of(2026, 5, 12, 10, 0, 0, 0, ZoneOffset.UTC)
      OffsetDateTimeDecoder.decode(schema, OffsetDateTimeEncoder.encode(schema, input)) shouldBe input
   }

   test("decoded OffsetDateTime always carries UTC offset") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMillis().addToSchema(schema)
      OffsetDateTimeDecoder.decode(schema, 0L).offset shouldBe ZoneOffset.UTC
   }
})
