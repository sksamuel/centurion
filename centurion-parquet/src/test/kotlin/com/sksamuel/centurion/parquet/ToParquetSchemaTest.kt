package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.parquet.schemas.ToParquetSchema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

class ToParquetSchemaTest : FunSpec() {

  init {

    test("Records should be converted to parquet message type") {

      val struct = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Booleans),
        Schema.Field("c", Schema.Float64),
        Schema.Field("d", Schema.Int64)
      )

      ToParquetSchema.toMessageType(struct) shouldBe
        Types.buildMessage()
          .addField(
            Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
              .`as`(LogicalTypeAnnotation.stringType()).named("a")
          )
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL).named("b"))
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL).named("c"))
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL).named("d"))
          .named("myrecord")
    }

    test("Schema.Strings should be converted to Binary with LogicalTypeAnnotation.string") {
      ToParquetSchema.toParquetType(Schema.Strings, "a", true) shouldBe
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
          .`as`(LogicalTypeAnnotation.stringType()).named("a")
    }

    test("Schema.TimestampMillis should be converted to INT64 with LogicalTypeAnnotation.TimeUnit.MILLIS") {
      ToParquetSchema.toParquetType(Schema.TimestampMillis, "a", true) shouldBe
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
              .`as`(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named("a")
    }

    test("Schema.Bytes should be converted to BINARY") {
      ToParquetSchema.toParquetType(Schema.Bytes, "a", true) shouldBe
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).named("a")
    }

    test("nested structs") {

      val struct = Schema.Struct(
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

      val b = Types.buildGroup(Type.Repetition.OPTIONAL)
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL).named("c"))
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL).named("d"))
        .named("b")

      ToParquetSchema.toMessageType(struct) shouldBe
        Types.buildMessage()
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL).named("a"))
          .addField(b)
          .named("myrecord")
    }

    test("required fields") {

      val struct = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Bytes, false),
        Schema.Field("b", Schema.Booleans, false),
      )

      ToParquetSchema.toMessageType(struct) shouldBe
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
            Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
              .`as`(LogicalTypeAnnotation.enumType()).named("a")
          )
          .named("myrecord")
    }
  }
}
