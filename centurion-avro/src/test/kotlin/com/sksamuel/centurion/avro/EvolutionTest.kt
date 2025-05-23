package com.sksamuel.centurion.avro

import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryReader
import com.sksamuel.centurion.avro.io.BinaryWriter
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class EvolutionTest : FunSpec() {

   init {
      test("!evolve schema by adding a nullable field") {

         val writer = SchemaBuilder.record("foo").fields()
            .requiredString("a")
            .requiredBoolean("b")
            .endRecord()

         val reader = SchemaBuilder.record("foo").fields()
            .requiredString("a")
            .requiredBoolean("b")
            .name("c").type(Schema.create(Schema.Type.STRING)).withDefault("foo")
            .endRecord()

         data class Foo1(val a: String, val b: Boolean)
         data class Foo2(val a: String, val b: Boolean, val c: String)

         val encoder = ReflectionRecordEncoder<Foo1>(reader)

         val baos = ByteArrayOutputStream()
         BinaryWriter(
            schema = writer,
            out = baos,
            binaryEncoder = EncoderFactory.get().binaryEncoder(baos, null),
            encoder = encoder
         ).use {
            it.write(Foo1("a", true))
         }

         BinaryReader(
            writerSchema = writer,
            readerSchema = reader,
            input = ByteArrayInputStream(baos.toByteArray()),
            factory = DecoderFactory.get(),
            reuse = null,
            decoder = ReflectionRecordDecoder<Foo2>(writer)
         ).read() shouldBe Foo2("a", true, "foo")
      }
   }
}
