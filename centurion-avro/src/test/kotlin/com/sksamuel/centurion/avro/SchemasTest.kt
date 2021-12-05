package com.sksamuel.centurion.avro

import com.sksamuel.centurion.Schema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.LogicalTypes
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

  test("doubles") {
    Schemas.toAvro(Schema.Float64) shouldBe SchemaBuilder.builder().doubleType()
    Schemas.fromAvro(SchemaBuilder.builder().doubleType()) shouldBe Schema.Float64
  }

  test("floats") {
    Schemas.toAvro(Schema.Float32) shouldBe SchemaBuilder.builder().floatType()
    Schemas.fromAvro(SchemaBuilder.builder().floatType()) shouldBe Schema.Float32
  }

  test("arrays") {
    Schemas.toAvro(Schema.Array(Schema.Booleans)) shouldBe SchemaBuilder.builder().array()
      .items(SchemaBuilder.builder().booleanType())

    Schemas.fromAvro(SchemaBuilder.builder().array().items(SchemaBuilder.builder().booleanType())) shouldBe
      Schema.Array(Schema.Booleans)
  }

  test("structs") {

    val struct = Schema.Struct(
      "mystruct",
      Schema.Field("a", Schema.Strings),
      Schema.Field("b", Schema.Booleans),
      Schema.Field("c", Schema.Float64),
      Schema.Field("d", Schema.Int64)
    )

    val avro = SchemaBuilder.record("mystruct").fields()
      .name("a").type(SchemaBuilder.builder().stringType()).noDefault()
      .name("b").type(SchemaBuilder.builder().booleanType()).noDefault()
      .name("c").type(SchemaBuilder.builder().doubleType()).noDefault()
      .name("d").type(SchemaBuilder.builder().longType()).noDefault()
      .endRecord()

    Schemas.toAvro(struct) shouldBe avro
    Schemas.fromAvro(avro) shouldBe struct
  }

  test("nested structs") {

    val struct = Schema.Struct(
      "mystruct",
      Schema.Field("a", Schema.Booleans),
      Schema.Field(
        "b", Schema.Struct(
          "nestedstruct",
          Schema.Field("x", Schema.Booleans),
          Schema.Field("y", Schema.Float32)
        )
      ),
      Schema.Field("c", Schema.Int64)
    )

    val avro = SchemaBuilder.record("mystruct").fields()
      .name("a").type(SchemaBuilder.builder().booleanType()).noDefault()
      .name("b").type(
        SchemaBuilder.builder().record("nestedstruct").fields()
          .name("x").type(SchemaBuilder.builder().booleanType()).noDefault()
          .name("y").type(SchemaBuilder.builder().floatType()).noDefault()
          .endRecord()
      ).noDefault()
      .name("c").type(SchemaBuilder.builder().longType()).noDefault()
      .endRecord()

    Schemas.toAvro(struct) shouldBe avro
    Schemas.fromAvro(avro) shouldBe struct
  }

  test("enums") {

    val schema = Schema.Enum("malbec", "pinot", "shiraz")
    val avro = SchemaBuilder.enumeration("enum").symbols("malbec", "pinot", "shiraz")

    Schemas.toAvro(schema) shouldBe avro
    Schemas.fromAvro(avro) shouldBe schema
  }

  test("decimals") {
    Schemas.toAvro(Schema.Decimal(Schema.Precision(5), Schema.Scale(2))) shouldBe
      LogicalTypes.decimal(5, 2).addToSchema(SchemaBuilder.builder().bytesType())

    Schemas.fromAvro(LogicalTypes.decimal(5, 2).addToSchema(SchemaBuilder.builder().bytesType())) shouldBe
      Schema.Decimal(Schema.Precision(5), Schema.Scale(2))
  }
})
