package com.sksamuel.centurion.avro.decoders

import com.sksamuel.centurion.avro.encoders.JavaStringUUIDEncoder
import com.sksamuel.centurion.avro.encoders.Utf8UUIDEncoder
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import java.util.UUID

class UUIDDecoderTest : FunSpec({

   val uuid: UUID = UUID.fromString("c0a80101-1d3e-11ed-861d-0242ac120002")
   val stringSchema = Schema.create(Schema.Type.STRING)
   val bytesSchema = Schema.create(Schema.Type.BYTES)
   val fixedSchema: Schema = SchemaBuilder.builder().fixed("uuid").size(16)

   test("decodes a Kotlin String") {
      UUIDDecoder.decode(stringSchema, uuid.toString()) shouldBe uuid
   }

   test("decodes a Utf8 (CharSequence)") {
      UUIDDecoder.decode(stringSchema, Utf8(uuid.toString())) shouldBe uuid
   }

   test("round-trips through Utf8UUIDEncoder") {
      UUIDDecoder.decode(stringSchema, Utf8UUIDEncoder.encode(stringSchema, uuid)) shouldBe uuid
   }

   test("round-trips through JavaStringUUIDEncoder") {
      UUIDDecoder.decode(stringSchema, JavaStringUUIDEncoder.encode(stringSchema, uuid)) shouldBe uuid
   }

   test("decodes from a 16-byte ByteArray (big-endian)") {
      val bytes = ByteBuffer.allocate(16).putLong(uuid.mostSignificantBits).putLong(uuid.leastSignificantBits).array()
      UUIDDecoder.decode(bytesSchema, bytes) shouldBe uuid
   }

   test("decodes from a positioned ByteBuffer") {
      val bytes = ByteBuffer.allocate(16).putLong(uuid.mostSignificantBits).putLong(uuid.leastSignificantBits).array()
      val padded = ByteBuffer.wrap(byteArrayOf(0, 0) + bytes)
      padded.position(2)
      UUIDDecoder.decode(bytesSchema, padded) shouldBe uuid
   }

   test("does not mutate the source ByteBuffer position") {
      val bytes = ByteBuffer.allocate(16).putLong(uuid.mostSignificantBits).putLong(uuid.leastSignificantBits).array()
      val buffer = ByteBuffer.wrap(bytes)
      val before = buffer.position()
      UUIDDecoder.decode(bytesSchema, buffer)
      buffer.position() shouldBe before
      buffer.remaining() shouldBe 16
   }

   test("decodes from a GenericFixed of size 16") {
      val bytes = ByteBuffer.allocate(16).putLong(uuid.mostSignificantBits).putLong(uuid.leastSignificantBits).array()
      val fixed = GenericData.get().createFixed(null, bytes, fixedSchema) as GenericData.Fixed
      UUIDDecoder.decode(fixedSchema, fixed) shouldBe uuid
   }

   test("rejects a byte array of the wrong length") {
      shouldThrow<IllegalArgumentException> {
         UUIDDecoder.decode(bytesSchema, byteArrayOf(1, 2, 3))
      }
   }

   test("rejects a non-supported value type") {
      shouldThrow<IllegalStateException> {
         UUIDDecoder.decode(stringSchema, 123)
      }
   }

   test("decoderFor returns UUIDDecoder for UUID") {
      Decoder.decoderFor(typeOfUuid(), stringSchema) shouldBe UUIDDecoder
   }
})

private fun typeOfUuid() = ::sample.returnType
private val sample: UUID get() = UUID.randomUUID()
