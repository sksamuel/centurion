package com.sksamuel.rxhive.parquet

import com.sksamuel.rxhive.StructField
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TimestampMillisType
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

class FromParquetSchemaTest : FunSpec() {
  init {
    test("should handle int 96") {
      val message = Types.buildMessage()
          .addField(
              Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, Type.Repetition.OPTIONAL).named("a")
          )
          .named("root")
      FromParquetSchema.fromParquet(message) shouldBe StructType(StructField("a", TimestampMillisType, true))
    }
  }
}