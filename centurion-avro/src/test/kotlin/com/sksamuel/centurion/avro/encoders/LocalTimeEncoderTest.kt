package com.sksamuel.centurion.avro.encoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.time.LocalTime
import java.util.concurrent.TimeUnit

class LocalTimeEncoderTest : FunSpec({

   val time = LocalTime.of(2, 3, 4, 123_456_000)

   test("time-millis logical type encodes as millis-of-day Int") {
      val schema = Schema.create(Schema.Type.INT)
      LogicalTypes.timeMillis().addToSchema(schema)
      LocalTimeEncoder.encode(schema, time) shouldBe TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay()).toInt()
   }

   test("time-micros logical type encodes as micros-of-day Long") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timeMicros().addToSchema(schema)
      LocalTimeEncoder.encode(schema, time) shouldBe TimeUnit.NANOSECONDS.toMicros(time.toNanoOfDay())
   }

   test("plain INT with no logical type encodes as millis-of-day") {
      val schema = Schema.create(Schema.Type.INT)
      LocalTimeEncoder.encode(schema, time) shouldBe TimeUnit.NANOSECONDS.toMillis(time.toNanoOfDay())
   }

   test("unsupported schema is rejected") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalStateException> {
         LocalTimeEncoder.encode(schema, time)
      }
   }
})
