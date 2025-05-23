package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

data class Foo(val a: String, val b: Int)

class BinaryWriterTest : FunSpec({

   test("round trip with multiple records") {

      val schema = ReflectionSchemaBuilder().schema(Foo::class)
      val baos = java.io.ByteArrayOutputStream()
      val writer = BinaryWriter<Foo>(
         schema = schema,
         out = baos,
         binaryEncoder = EncoderFactory.get().binaryEncoder(baos, null),
         encoder = ReflectionRecordEncoder(schema)
      )
      writer.write(Foo("hello", 123))
      writer.write(Foo("world", 456))
      writer.close()

      val reader = BinaryReader(
         schema = schema,
         input = java.io.ByteArrayInputStream(baos.toByteArray()),
         factory = DecoderFactory.get(),
         decoder = ReflectionRecordDecoder<Foo>(schema),
         reuse = null
      )

      reader.read() shouldBe Foo("hello", 123)
      reader.read() shouldBe Foo("world", 456)
      reader.close()
   }

})
