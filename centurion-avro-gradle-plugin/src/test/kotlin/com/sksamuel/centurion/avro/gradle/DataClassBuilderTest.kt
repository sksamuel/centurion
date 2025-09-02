package com.sksamuel.centurion.avro.gradle

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.resource.shouldMatchResource
import org.apache.avro.SchemaBuilder

class DataClassBuilderTest : FunSpec({

   test("record with primitives") {
      val schema = SchemaBuilder.builder().record("Foo").namespace("com.sksamuel.centurion")
         .fields()
         .name("a").type().intType().noDefault()
         .name("b").type().stringType().noDefault()
         .endRecord()
      DataClassBuilder().build(schema).toString().shouldMatchResource("/record_with_primitives.txt", trim = true)
   }

   test("record with primitive lists") {
      val schema = SchemaBuilder.builder().record("Foo").namespace("com.sksamuel.centurion")
         .fields()
         .name("a").type().intType().noDefault()
         .name("b").type().stringType().noDefault()
         .name("c").type().array().items(SchemaBuilder.builder().stringType()).noDefault()
         .endRecord()
      DataClassBuilder().build(schema).toString().shouldMatchResource("/record_with_primitive_lists.txt", trim = true)
   }
})
