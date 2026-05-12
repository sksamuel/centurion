package com.sksamuel.centurion.avro.encoders

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.time.Instant

class InstantEncoderTest : FunSpec({

   test("timestamp millis logical type encodes as epoch milli Long") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMillis().addToSchema(schema)
      InstantEncoder.encode(schema, Instant.ofEpochMilli(12_345L)) shouldBe 12_345L
   }

   test("timestamp micros logical type encodes as epoch micro Long") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMicros().addToSchema(schema)
      InstantEncoder.encode(schema, Instant.ofEpochMilli(12_345L)) shouldBe 12_345_000L
   }

   test("plain LONG with no logical type encodes as epoch millis") {
      val schema = Schema.create(Schema.Type.LONG)
      InstantEncoder.encode(schema, Instant.ofEpochMilli(12_345L)) shouldBe 12_345L
   }

   test("non-long schemas are rejected") {
      val schema = Schema.create(Schema.Type.INT)
      shouldThrow<IllegalStateException> {
         InstantEncoder.encode(schema, Instant.ofEpochMilli(1L))
      }
   }
})
