package com.sksamuel.reactivehive.parquet

import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.DoubleType
import com.sksamuel.reactivehive.FloatType
import com.sksamuel.reactivehive.LongType
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TimestampMillisType
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
          StructField("c", DoubleType),
          StructField("d", LongType)
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
              StructField("c", DoubleType),
              StructField("d", FloatType)
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
  }
}