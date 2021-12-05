package com.sksamuel.centurion.avro

import com.sksamuel.centurion.Schema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder

class SchemasTest : FunSpec({

  test("strings") {
    Schemas.toAvro(Schema.Strings) shouldBe SchemaBuilder.builder().stringType()
    Schemas.fromAvro(SchemaBuilder.builder().stringType()) shouldBe Schema.Strings
  }

  test("booleans") {
    Schemas.toAvro(Schema.Booleans) shouldBe SchemaBuilder.builder().booleanType()
    Schemas.fromAvro(SchemaBuilder.builder().booleanType()) shouldBe Schema.Booleans
  }

  test("bytes") {
    Schemas.toAvro(Schema.Bytes) shouldBe SchemaBuilder.builder().bytesType()
    Schemas.fromAvro(SchemaBuilder.builder().bytesType()) shouldBe Schema.Bytes
  }

  test("ints") {
    Schemas.toAvro(Schema.Int32) shouldBe SchemaBuilder.builder().intType()
    Schemas.fromAvro(SchemaBuilder.builder().intType()) shouldBe Schema.Int32
  }

  test("longs") {
    Schemas.toAvro(Schema.Int64) shouldBe SchemaBuilder.builder().longType()
    Schemas.fromAvro(SchemaBuilder.builder().longType()) shouldBe Schema.Int64
  }

  test("arrays") {
    Schemas.toAvro(Schema.Array(Schema.Booleans)) shouldBe SchemaBuilder.builder().array()
      .items(SchemaBuilder.builder().booleanType())

    Schemas.fromAvro(SchemaBuilder.builder().array().items(SchemaBuilder.builder().booleanType())) shouldBe
      Schema.Array(Schema.Booleans)
  }
})
