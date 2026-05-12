package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.encoders.LocalDateTimeEncoder
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.time.LocalDateTime

class LocalDateTimeDecoderTest : FunSpec({

   test("local timestamp millis") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.localTimestampMillis().addToSchema(schema)
      val input = LocalDateTime.of(2026, 5, 12, 10, 20, 30)
      LocalDateTimeDecoder.decode(schema, LocalDateTimeEncoder.encode(schema, input)) shouldBe input
   }

   test("local timestamp micros") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.localTimestampMicros().addToSchema(schema)
      val input = LocalDateTime.of(2026, 5, 12, 10, 20, 30, 123_456_000)
      LocalDateTimeDecoder.decode(schema, LocalDateTimeEncoder.encode(schema, input)) shouldBe input
   }

   test("long with no logical type is interpreted as epoch millis at UTC") {
      val schema = Schema.create(Schema.Type.LONG)
      val input = LocalDateTime.of(2026, 5, 12, 10, 20, 30)
      LocalDateTimeDecoder.decode(schema, LocalDateTimeEncoder.encode(schema, input)) shouldBe input
   }

   test("unsupported value type fails with a helpful error") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.localTimestampMillis().addToSchema(schema)
      shouldThrow<IllegalStateException> {
         LocalDateTimeDecoder.decode(schema, "not a long")
      }
   }

   test("unsupported logical type on a long fails with a helpful error") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timeMicros().addToSchema(schema)
      shouldThrow<IllegalStateException> {
         LocalDateTimeDecoder.decode(schema, 12345L)
      }
   }
})
