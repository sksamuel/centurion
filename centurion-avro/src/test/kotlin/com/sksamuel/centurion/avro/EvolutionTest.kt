package com.sksamuel.centurion.avro

import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryReader
import com.sksamuel.centurion.avro.io.BinaryWriter
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecord
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
            output = baos,
            factory = EncoderFactory.get(),
            reuse = null,
         ).use {
            val record = encoder.encode(writer, Foo1("a", true)) as GenericRecord
            it.write(record)
         }

         BinaryReader(
            writerSchema = writer,
            readerSchema = reader,
            input = ByteArrayInputStream(baos.toByteArray()),
            factory = DecoderFactory.get(),
            reuse = null,
            decoder = ReflectionRecordDecoder<Foo2>()
         ).read() shouldBe Foo2("a", true, "foo")
      }
   }
}
