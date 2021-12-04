package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

class ToParquetSchemaTest : FunSpec() {

  init {

    test("Records should be converted to parquet message type") {

      val record = Schema.Record(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Booleans),
        Schema.Field("c", Schema.Float64),
        Schema.Field("d", Schema.Int64)
      )

      ToParquetSchema.toMessageType(record) shouldBe
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

//    test("TimestampMillisType should be converted to INT64 with original type TIMESTAMP_MILLIS") {
//      ToParquetSchema.toParquetType(TimestampMillisType, "a", true) shouldBe
//          Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
//              .`as`(OriginalType.TIMESTAMP_MILLIS).named("a")
//    }

    test("Schema.Bytes should be converted to BINARY") {
      ToParquetSchema.toParquetType(Schema.Bytes, "a", true) shouldBe
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).named("a")
    }

    test("nested structs") {

      val record = Schema.Record(
        "myrecord",
        Schema.Field("a", Schema.Booleans),
        Schema.Field(
          "b",
          Schema.Record(
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

      ToParquetSchema.toMessageType(record) shouldBe
        Types.buildMessage()
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL).named("a"))
          .addField(b)
          .named("myrecord")
    }

    test("required fields") {

      val record = Schema.Record(
        "myrecord",
        Schema.Field("a", Schema.Bytes, false),
        Schema.Field("b", Schema.Booleans, false),
      )

      ToParquetSchema.toMessageType(record) shouldBe
        Types.buildMessage()
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("a"))
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("b"))
          .named("myrecord")
    }

    test("Schema.Enum should be converted to annotated Binary") {

      val record = Schema.Record(
        "myrecord",
        Schema.Field("a", Schema.Enum("malbec", "pinot noir")),
      )

      ToParquetSchema.toMessageType(record) shouldBe
        Types.buildMessage()
          .addField(
            Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
              .`as`(LogicalTypeAnnotation.enumType()).named("a")
          )
          .named("myrecord")
    }
  }
}
