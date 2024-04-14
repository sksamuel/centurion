package com.sksamuel.centurion.avro.generation

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder

class SchemaBuilderGeneratorTest : FunSpec({

   test("build schema with primitives") {

      val schema = SchemaBuilder.record("Foo")
         .namespace("a.b")
         .fields()
         .requiredString("a")
         .requiredBoolean("b")
         .optionalLong("c")
         .endRecord()

      SchemaBuilderGenerator().generate(schema).trim() shouldBe """
package a.b

import com.sksamuel.centurion.avro.generation.GenericRecordEncoder
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

val schemaFoo = SchemaBuilder.record("Foo").namespace("a.b")
.fields()
.name("a").type().stringType().noDefault()
.name("b").type().booleanType().noDefault()
.name("c").type().optional().longType()
.endRecord()
""".trim()
   }

})
