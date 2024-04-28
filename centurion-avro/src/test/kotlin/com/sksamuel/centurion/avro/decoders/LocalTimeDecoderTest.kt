package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.encoders.LocalTimeEncoder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.time.LocalTime

class LocalTimeDecoderTest : FunSpec({

   test("time millis") {
      val schema = Schema.create(Schema.Type.INT)
      LogicalTypes.timeMillis().addToSchema(schema)
      val input = LocalTime.of(2, 3, 4)
      LocalTimeDecoder.decode(schema).invoke(LocalTimeEncoder.encode(schema).invoke(input)) shouldBe input
   }

   test("time micros") {
      val schema = Schema.create(Schema.Type.LONG)
      LogicalTypes.timeMicros().addToSchema(schema)
      val input = LocalTime.of(2, 3, 4)
      LocalTimeDecoder.decode(schema).invoke(LocalTimeEncoder.encode(schema).invoke(input)) shouldBe input
   }

   test("longs as millis") {
      val schema = Schema.create(Schema.Type.INT)
      val input = LocalTime.of(2, 3, 4)
      LocalTimeDecoder.decode(schema).invoke(LocalTimeEncoder.encode(schema).invoke(input)) shouldBe input
   }

})
