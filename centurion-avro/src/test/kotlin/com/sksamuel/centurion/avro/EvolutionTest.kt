package com.sksamuel.centurion.avro

import com.sksamuel.centurion.avro.io.BinaryReaderFactory
import com.sksamuel.centurion.avro.io.BinaryWriterFactory
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

class EvolutionTest : FunSpec() {

   init {
      test("evolve schema by adding a nullable field") {

         val schema1 = SchemaBuilder.record("foo").fields()
            .requiredString("a")
            .requiredBoolean("b")
            .endRecord()

         val schema2 = SchemaBuilder.record("foo").fields()
            .requiredString("a")
            .requiredBoolean("b")
            .optionalString("c")
            .endRecord()

         val record1 = GenericData.Record(schema1)
         record1.put("a", "hello")
         record1.put("b", true)

         val bytes = BinaryWriterFactory.write(record1)
         val record2 = BinaryReaderFactory(schema2, schema1).read(bytes)

         record2["a"] shouldBe Utf8("hello")
         record2["b"] shouldBe true
         record2["c"] shouldBe null

      }
   }


}
