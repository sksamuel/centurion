package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.nullable
import com.sksamuel.centurion.parquet.schemas.FromParquetSchema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

class FromParquetSchemaTest : FunSpec() {
  init {

    test("integrals") {
      FromParquetSchema.fromParquet(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
        .`as`(LogicalTypeAnnotation.intType(8, true)).named("a")) shouldBe Schema.Int8

      FromParquetSchema.fromParquet(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
        .`as`(LogicalTypeAnnotation.intType(16, true)).named("b")) shouldBe Schema.Int16
    }

    test("should handle deprecated int 96") {
      val message = Types.buildMessage().addField(
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, Type.Repetition.REQUIRED).named("a")
      ).named("root")
      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct("root", Schema.Field("a", Schema.TimestampMillis))
    }

    test("handle Type.Repetition.OPTIONAL") {

      val message = Types
        .buildMessage()
        .addField(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("a"))
        .named("root")

      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct(
        "root",
        Schema.Field("a", Schema.Booleans.nullable())
      )
    }

    test("required fields") {

      val message = Types.buildMessage()
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("a"))
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("b"))
        .named("myrecord")

      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Bytes),
        Schema.Field("b", Schema.Booleans),
      )
    }

    test("BINARY should be converted to Schema.Bytes") {
      FromParquetSchema.fromPrimitiveType(
        Types.primitive(
          PrimitiveType.PrimitiveTypeName.BINARY,
          Type.Repetition.OPTIONAL
        ).named("a")
      ) shouldBe Schema.Bytes.nullable()
    }

    test("Logical type String should be converted to Schema.Strings") {
      FromParquetSchema.fromPrimitiveType(
        Types.primitive(
          PrimitiveType.PrimitiveTypeName.BINARY,
          Type.Repetition.REQUIRED
        ).`as`(LogicalTypeAnnotation.stringType()).named("a")
      ) shouldBe Schema.Strings
    }

    test("Logical type String with length should be converted to Schema.Varchar") {
      FromParquetSchema.fromPrimitiveType(
        Types.primitive(
          PrimitiveType.PrimitiveTypeName.BINARY,
          Type.Repetition.OPTIONAL
        ).length(215).`as`(LogicalTypeAnnotation.stringType()).named("a")
      ) shouldBe Schema.Varchar(215).nullable()
    }

    test("maps") {

      val message = Types.buildMessage()
        .addField(
          Types.map(Type.Repetition.REQUIRED)
            .key(Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("key"))
            .value(Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("value")).named("a")
        ).named("myrecord")

      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Map(Schema.Booleans, Schema.Float64)),
      )
    }

    test("optional maps") {

      val message = Types.buildMessage()
        .addField(
          Types.map(Type.Repetition.OPTIONAL)
            .key(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("key"))
            .value(Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("value"))
            .named("a")
        ).named("myrecord")

      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Map(Schema.Int32, Schema.Booleans).nullable()),
      )
    }

    test("optional map values") {

      val message = Types.buildMessage()
        .addField(
          Types.map(Type.Repetition.REPEATED)
            .key(Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("key"))
            .value(Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("value"))
            .named("a")
        ).named("myrecord")

      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Map(Schema.Int32, Schema.Booleans.nullable())),
      )
    }

    test("arrays of booleans") {

      val message = Types.buildMessage().addField(
        Types.list(Type.Repetition.REQUIRED).element(
          Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("b")
        ).named("a")
      ).named("myrecord")

      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Array(Schema.Int32)),
      )
    }

    test("arrays of structs") {

      val x = Types.buildGroup(Type.Repetition.REQUIRED)
        .addField(Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("c"))
        .addField(Types.required(PrimitiveType.PrimitiveTypeName.FLOAT).named("d"))
        .named("x")

      val y = Types.buildMessage()
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("a"))
        .addField(Types.list(Type.Repetition.REQUIRED).element(x).named("b"))
        .named("y")

      FromParquetSchema.fromParquet(y) shouldBe Schema.Struct(
        "y",
        Schema.Field("a", Schema.Booleans),
        Schema.Field(
          "b", Schema.Array(
            Schema.Struct(
              "x",
              Schema.Field("c", Schema.Int32.nullable()),
              Schema.Field("d", Schema.Float32),
            )
          )
        ),
      )
    }

    test("optional arrays of primitives") {

      val message = Types.buildMessage().addField(
        Types.list(Type.Repetition.OPTIONAL).element(
          Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("b")
        ).named("a")
      ).named("myrecord")

      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Array(Schema.Int32).nullable()),
      )
    }

    test("decimals") {
      FromParquetSchema.fromParquet(
        Types.required(PrimitiveType.PrimitiveTypeName.INT32).`as`(LogicalTypeAnnotation.decimalType(2, 8)).named("a")
      ) shouldBe Schema.Decimal(Schema.Precision(8), Schema.Scale(2))

      FromParquetSchema.fromParquet(
        Types.required(PrimitiveType.PrimitiveTypeName.INT64).`as`(LogicalTypeAnnotation.decimalType(2, 14)).named("a")
      ) shouldBe Schema.Decimal(Schema.Precision(14), Schema.Scale(2))
    }

    test("enums") {
      FromParquetSchema.fromParquet(
        Types.required(PrimitiveType.PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.enumType()).named("enum")
      ) shouldBe Schema.Enum(emptyList())

    }

    test("timestamps") {

      FromParquetSchema.fromParquet(
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
          .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)).named("x")
      ) shouldBe Schema.TimestampMicros

      FromParquetSchema.fromParquet(
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
          .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("x")
      ) shouldBe Schema.TimestampMillis
    }
  }
}
