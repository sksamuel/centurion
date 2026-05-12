package com.sksamuel.centurion.avro.encoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import java.util.UUID

class UUIDEncoderTest : FunSpec({

   val uuid: UUID = UUID.fromString("c0a80101-1d3e-11ed-861d-0242ac120002")

   test("Utf8UUIDEncoder encodes as a Utf8") {
      Utf8UUIDEncoder.encode(Schema.create(Schema.Type.STRING), uuid) shouldBe Utf8(uuid.toString())
   }

   test("JavaStringUUIDEncoder encodes as a Java String") {
      JavaStringUUIDEncoder.encode(Schema.create(Schema.Type.STRING), uuid) shouldBe uuid.toString()
   }
})
