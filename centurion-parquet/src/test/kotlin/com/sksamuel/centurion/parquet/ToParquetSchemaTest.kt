package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.BinaryType
import com.sksamuel.centurion.BooleanType
import com.sksamuel.centurion.EnumType
import com.sksamuel.centurion.Float64Type
import com.sksamuel.centurion.Float32Type
import com.sksamuel.centurion.Int64Type
import com.sksamuel.centurion.StringType
import com.sksamuel.centurion.StructField
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.TimestampMillisType
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

class ToParquetSchemaTest : FunSpec() {

  init {

    test("Struct should be converted to parquet message type") {
      val structType = StructType(
          StructField("a", StringType),
          StructField("b", BooleanType),
          StructField("c", Float64Type),
          StructField("d", Int64Type)
      )
      ToParquetSchema.toMessageType(structType, "mystruct") shouldBe
          Types.buildMessage()
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                  .`as`(OriginalType.UTF8).named("a"))
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL).named("b"))
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL).named("c"))
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL).named("d"))
              .named("mystruct")
    }

    test("StringType should be converted to Binary with original type UTF8") {
      ToParquetSchema.toParquetType(StringType, "a", true) shouldBe
          Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
              .`as`(OriginalType.UTF8).named("a")
    }

    test("TimestampMillisType should be converted to INT64 with original type TIMESTAMP_MILLIS") {
      ToParquetSchema.toParquetType(TimestampMillisType, "a", true) shouldBe
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
              .`as`(OriginalType.TIMESTAMP_MILLIS).named("a")
    }

    test("BinaryType should be converted to BINARY") {
      ToParquetSchema.toParquetType(BinaryType, "a", true) shouldBe
          Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).named("a")
    }

    test("nested structs") {
      val structType = StructType(
          StructField("a", BooleanType),
          StructField("b", StructType(
              StructField("c", Float64Type),
              StructField("d", Float32Type)
          ))
      )

      val b = Types.buildGroup(Type.Repetition.OPTIONAL)
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL).named("c"))
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL).named("d"))
          .named("b")

      ToParquetSchema.toMessageType(structType, "mystruct") shouldBe
          Types.buildMessage()
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL).named("a"))
              .addField(b)
              .named("mystruct")
    }

    test("required fields") {
      val structType = StructType(
          StructField("a", BinaryType, false),
          StructField("b", BooleanType, false)
      )
      ToParquetSchema.toMessageType(structType, "mystruct") shouldBe
          Types.buildMessage()
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("a"))
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("b"))
              .named("mystruct")
    }

    test("EnumType should be converted to annotated Binary") {
      val structType = StructType(
          StructField("a", EnumType("malbec", "shiraz"))
      )
      ToParquetSchema.toMessageType(structType, "mystruct") shouldBe
          Types.buildMessage()
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                  .`as`(OriginalType.ENUM).named("a"))
              .named("mystruct")
    }
  }
}
