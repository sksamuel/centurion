//package com.sksamuel.centurion.parquet
//
//import com.sksamuel.centurion.StructField
//import com.sksamuel.centurion.StructType
//import com.sksamuel.centurion.TimestampMillisType
//import io.kotest.core.spec.style.FunSpec
//import io.kotest.matchers.shouldBe
//import org.apache.parquet.schema.PrimitiveType
//import org.apache.parquet.schema.Type
//import org.apache.parquet.schema.Types
//
//class FromParquetSchemaTest : FunSpec() {
//  init {
//    test("should handle int 96") {
//      val message = Types.buildMessage()
//          .addField(
//              Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, Type.Repetition.OPTIONAL).named("a")
//          )
//          .named("root")
//      FromParquetSchema.fromParquet(message) shouldBe StructType(StructField("a", TimestampMillisType, true))
//    }
//  }
//}
