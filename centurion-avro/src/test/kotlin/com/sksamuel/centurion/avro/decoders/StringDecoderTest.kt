package com.sksamuel.centurion.avro.decoders

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

class StringDecoderTest : FunSpec({

   test("decode java strings") {
      val schema = SchemaBuilder.builder().stringType()
      StringDecoder.decode(schema, "foo") shouldBe "foo"
   }

   test("decode UTF8") {
      val schema = SchemaBuilder.builder().stringType()
      StringDecoder.decode(schema, Utf8("foo")) shouldBe "foo"
   }

   test("decode byte buffer") {
      val schema = SchemaBuilder.builder().stringType()
      StringDecoder.decode(schema, ByteBuffer.wrap("foo".encodeToByteArray())) shouldBe "foo"
   }

   test("decode byte array") {
      val schema = SchemaBuilder.builder().bytesType()
      StringDecoder.decode(schema, "foo".encodeToByteArray()) shouldBe "foo"
   }

   test("decode fixed") {
      val schema = SchemaBuilder.fixed("name").size(3)
      StringDecoder.decode(
         schema,
         GenericData.get().createFixed(null, "foo".encodeToByteArray(), schema)
      ) shouldBe "foo"
   }

   test("decode byte buffer that has been advanced past leading bytes") {
      val schema = SchemaBuilder.builder().stringType()
      val buffer = ByteBuffer.wrap("xxxfoo".encodeToByteArray())
      buffer.position(3)
      StringDecoder.decode(schema, buffer) shouldBe "foo"
   }

   test("decode byte buffer with limit set before the end of the backing array") {
      val schema = SchemaBuilder.builder().stringType()
      val buffer = ByteBuffer.wrap("fooyyy".encodeToByteArray())
      buffer.limit(3)
      StringDecoder.decode(schema, buffer) shouldBe "foo"
   }

   test("decode a sliced byte buffer that shares its backing array") {
      val schema = SchemaBuilder.builder().stringType()
      val backing = ByteBuffer.wrap("xxxfooyyy".encodeToByteArray())
      backing.position(3)
      backing.limit(6)
      val slice = backing.slice()
      StringDecoder.decode(schema, slice) shouldBe "foo"
   }

})

class UTF8DecoderTest : FunSpec({

   test("decode java strings") {
      val schema = SchemaBuilder.builder().stringType()
      UTF8Decoder.decode(schema, "foo") shouldBe Utf8("foo")
   }

   test("decode byte array") {
      val schema = SchemaBuilder.builder().bytesType()
      UTF8Decoder.decode(schema, "foo".encodeToByteArray()) shouldBe Utf8("foo")
   }

   test("decode byte buffer") {
      val schema = SchemaBuilder.builder().bytesType()
      UTF8Decoder.decode(schema, ByteBuffer.wrap("foo".encodeToByteArray())) shouldBe Utf8("foo")
   }

   test("decode byte buffer that has been advanced past leading bytes") {
      val schema = SchemaBuilder.builder().bytesType()
      val buffer = ByteBuffer.wrap("xxxfoo".encodeToByteArray())
      buffer.position(3)
      UTF8Decoder.decode(schema, buffer) shouldBe Utf8("foo")
   }

   test("decode a sliced byte buffer that shares its backing array") {
      val schema = SchemaBuilder.builder().bytesType()
      val backing = ByteBuffer.wrap("xxxfooyyy".encodeToByteArray())
      backing.position(3)
      backing.limit(6)
      val slice = backing.slice()
      UTF8Decoder.decode(schema, slice) shouldBe Utf8("foo")
   }

})

class ByteStringDecoderTest : FunSpec({

   test("decode byte array") {
      val schema = SchemaBuilder.builder().bytesType()
      ByteStringDecoder.decode(schema, "foo".encodeToByteArray()) shouldBe "foo"
   }

   test("decode byte buffer") {
      val schema = SchemaBuilder.builder().bytesType()
      ByteStringDecoder.decode(schema, ByteBuffer.wrap("foo".encodeToByteArray())) shouldBe "foo"
   }

   test("decode byte buffer that has been advanced past leading bytes") {
      val schema = SchemaBuilder.builder().bytesType()
      val buffer = ByteBuffer.wrap("xxxfoo".encodeToByteArray())
      buffer.position(3)
      ByteStringDecoder.decode(schema, buffer) shouldBe "foo"
   }

   test("decode a sliced byte buffer that shares its backing array") {
      val schema = SchemaBuilder.builder().bytesType()
      val backing = ByteBuffer.wrap("xxxfooyyy".encodeToByteArray())
      backing.position(3)
      backing.limit(6)
      val slice = backing.slice()
      ByteStringDecoder.decode(schema, slice) shouldBe "foo"
   }

})
