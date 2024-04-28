package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.encoders.InstantEncoder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.time.Instant

class InstantDecoderTest : FunSpec({

   test("timestamp millis") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMillis().addToSchema(schema)
      val input = Instant.ofEpochMilli(12345)
      InstantDecoder.decode(schema, InstantEncoder.encode(schema).invoke(input)) shouldBe input
   }

   test("timestamp micros") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timestampMicros().addToSchema(schema)
      val input = Instant.ofEpochMilli(12345)
      InstantDecoder.decode(schema, InstantEncoder.encode(schema).invoke(input)) shouldBe input
   }

   test("instant as longs") {
      val schema = Schema.create(Schema.Type.LONG)
      val input = Instant.ofEpochMilli(12345)
      InstantDecoder.decode(schema, InstantEncoder.encode(schema).invoke(input)) shouldBe input
   }

})
