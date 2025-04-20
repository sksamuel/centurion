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

})
