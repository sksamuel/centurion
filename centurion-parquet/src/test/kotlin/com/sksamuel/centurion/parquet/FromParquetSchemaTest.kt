package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.parquet.schemas.FromParquetSchema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

class FromParquetSchemaTest : FunSpec() {
  init {

    test("should handle deprecated int 96") {
      val message = Types.buildMessage().addField(
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, Type.Repetition.OPTIONAL).named("a")
      ).named("root")
      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct("root", Schema.Field("a", Schema.TimestampMillis))
    }

    test("required fields") {

      val message = Types.buildMessage()
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("a"))
        .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("b"))
        .named("myrecord")

      FromParquetSchema.fromParquet(message) shouldBe Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Bytes, false),
        Schema.Field("b", Schema.Booleans, false),
      )
    }

    test("BINARY should be converted to Schema.Bytes") {
      FromParquetSchema.fromPrimitiveType(
        Types.primitive(
          PrimitiveType.PrimitiveTypeName.BINARY,
          Type.Repetition.OPTIONAL
        ).named("a")
      ) shouldBe Schema.Bytes
    }

    test("Logical type String should be converted to Schema.Strings") {
      FromParquetSchema.fromPrimitiveType(
        Types.primitive(
          PrimitiveType.PrimitiveTypeName.BINARY,
          Type.Repetition.OPTIONAL
        ).`as`(LogicalTypeAnnotation.stringType()).named("a")
      ) shouldBe Schema.Strings
    }

    test("Logical type String with length should be converted to Schema.Varchar") {
      FromParquetSchema.fromPrimitiveType(
        Types.primitive(
          PrimitiveType.PrimitiveTypeName.BINARY,
          Type.Repetition.OPTIONAL
        ).length(215).`as`(LogicalTypeAnnotation.stringType()).named("a")
      ) shouldBe Schema.Varchar(215)
    }
  }
}
