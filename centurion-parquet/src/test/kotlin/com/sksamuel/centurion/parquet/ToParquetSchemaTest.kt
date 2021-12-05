package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.nullable
import com.sksamuel.centurion.parquet.schemas.ToParquetSchema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

class ToParquetSchemaTest : FunSpec() {

  init {

    test("structs should be converted to parquet message type") {

      val struct = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Booleans.nullable()),
        Schema.Field("c", Schema.Float64),
        Schema.Field("d", Schema.Int64.nullable())
      )

      ToParquetSchema.toMessageType(struct) shouldBe
        Types.buildMessage()
          .addField(
            Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
              .`as`(LogicalTypeAnnotation.stringType()).named("a")
          )
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL).named("b"))
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED).named("c"))
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL).named("d"))
          .named("myrecord")
    }

    test("Schema.Strings should be converted to Binary with LogicalTypeAnnotation.string") {
      ToParquetSchema.toParquetType(Schema.Strings, "a") shouldBe
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
          .`as`(LogicalTypeAnnotation.stringType()).named("a")
    }

    test("Schema.TimestampMillis should be converted to INT64 with LogicalTypeAnnotation.TimeUnit.MILLIS") {
      ToParquetSchema.toParquetType(Schema.TimestampMillis, "a") shouldBe
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
          .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("a")
    }

    test("Schema.Bytes should be converted to BINARY") {
      ToParquetSchema.toParquetType(Schema.Bytes, "a") shouldBe
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("a")
    }

    test("nested structs") {

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Booleans),
        Schema.Field(
          "b",
          Schema.Struct(
            "myrecord2",
            Schema.Field("c", Schema.Float64),
            Schema.Field("d", Schema.Float32)
          )
        )
      )

      val b = Types.buildGroup(Type.Repetition.REQUIRED)
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED).named("c"))
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.REQUIRED).named("d"))
        .named("b")

      ToParquetSchema.toMessageType(schema) shouldBe
        Types.buildMessage()
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("a"))
          .addField(b)
          .named("myrecord")
    }

    test("arrays of primitives") {

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Array(Schema.Int32)),
      )

      ToParquetSchema.toMessageType(schema) shouldBe Types.buildMessage()
        .addField(
          Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .`as`(LogicalTypeAnnotation.stringType()).named("a")
        )
        .addField(
          Types.list(Type.Repetition.REQUIRED)
            .element(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).named("element"))
            .named("b")
        )
        .named("myrecord")
    }

    test("optional arrays of primitives") {

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Array(Schema.Int32).nullable()),
      )

      ToParquetSchema.toMessageType(schema) shouldBe Types.buildMessage()
        .addField(
          Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .`as`(LogicalTypeAnnotation.stringType()).named("a")
        )
        .addField(
          Types.list(Type.Repetition.OPTIONAL)
            .element(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).named("element"))
            .named("b")
        )
        .named("myrecord")
    }

    test("arrays of structs") {

      val schema = Schema.Struct(
        "x",
        Schema.Field("a", Schema.Bytes),
        Schema.Field("b", Schema.Array(Schema.Struct("y", Schema.Field("c", Schema.Booleans))))
      )

      ToParquetSchema.toMessageType(schema) shouldBe Types.buildMessage()
        .addField(Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("a"))
        .addField(
          Types.list(Type.Repetition.REQUIRED).element(
            Types
              .buildGroup(Type.Repetition.REQUIRED)
              .addField(Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("c"))
              .named("element")
          ).named("b")
        ).named("x")
    }

    test("required fields") {

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Bytes),
        Schema.Field("b", Schema.Booleans),
      )

      ToParquetSchema.toMessageType(schema) shouldBe
        Types.buildMessage()
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("a"))
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("b"))
          .named("myrecord")
    }

    test("Schema.Enum should be converted to annotated Binary") {

      val struct = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Enum("malbec", "pinot noir")),
      )

      ToParquetSchema.toMessageType(struct) shouldBe
        Types.buildMessage()
          .addField(
            Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
              .`as`(LogicalTypeAnnotation.enumType()).named("a")
          ).named("myrecord")
    }

    test("maps of booleans") {

      val struct = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Map(Schema.Booleans)),
      )

      ToParquetSchema.toMessageType(struct) shouldBe
        Types.buildMessage()
          .addField(
            Types.map(Type.Repetition.REQUIRED)
              .key(
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                  .`as`(LogicalTypeAnnotation.stringType()).named("key")
              ).value(
                Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("value")
              ).named("a")
          ).named("myrecord")
    }

    test("optional maps of booleans") {

      val struct = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Map(Schema.Booleans).nullable()),
      )

      ToParquetSchema.toMessageType(struct) shouldBe
        Types.buildMessage()
          .addField(
            Types.map(Type.Repetition.OPTIONAL)
              .key(
                Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                  .`as`(LogicalTypeAnnotation.stringType()).named("key")
              ).value(
                Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("value")
              ).named("a")
          ).named("myrecord")
    }

    test("decimals") {
      ToParquetSchema.toParquetType(Schema.Decimal(Schema.Precision(5), Schema.Scale(2)), "a") shouldBe
        Types.required(PrimitiveType.PrimitiveTypeName.INT32).`as`(LogicalTypeAnnotation.decimalType(2, 5)).named("a")

      ToParquetSchema.toParquetType(Schema.Decimal(Schema.Precision(14), Schema.Scale(2)), "a") shouldBe
        Types.required(PrimitiveType.PrimitiveTypeName.INT64).`as`(LogicalTypeAnnotation.decimalType(2, 14)).named("a")
    }

    test("enums") {
      val schema = Schema.Enum("malbec", "pinot", "shiraz")
      ToParquetSchema.toParquetType(schema, "enum") shouldBe
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
          .`as`(LogicalTypeAnnotation.enumType())
          .named("enum")
    }

    test("varchars") {
      ToParquetSchema.toParquetType(Schema.Varchar(123), "a") shouldBe
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
          .`as`(LogicalTypeAnnotation.stringType())
          .named("a")
    }
  }
}
