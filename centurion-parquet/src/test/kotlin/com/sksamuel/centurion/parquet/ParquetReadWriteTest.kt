//package com.sksamuel.centurion.parquet
//
//import com.sksamuel.centurion.BooleanType
//import com.sksamuel.centurion.DateType
//import com.sksamuel.centurion.EnumType
//import com.sksamuel.centurion.Float32Type
//import com.sksamuel.centurion.Float64Type
//import com.sksamuel.centurion.Int16Type
//import com.sksamuel.centurion.Int32Type
//import com.sksamuel.centurion.Int64Type
//import com.sksamuel.centurion.Int8Type
//import com.sksamuel.centurion.StringType
//import com.sksamuel.centurion.Struct
//import com.sksamuel.centurion.StructField
//import com.sksamuel.centurion.StructType
//import com.sksamuel.centurion.TimeMillis
//import com.sksamuel.centurion.TimeMillisType
//import com.sksamuel.centurion.TimestampMillisType
//import io.kotest.core.spec.style.FunSpec
//import io.kotest.core.test.TestCase
//import io.kotest.core.test.TestResult
//import io.kotest.matchers.shouldBe
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.FileSystem
//import org.apache.hadoop.fs.Path
//import java.sql.Timestamp
//import java.time.LocalDate
//import java.time.LocalDateTime
//
//class ParquetReadWriteTest : FunSpec() {
//
//  private val path = Path("test.pq")
//
//  private val conf = Configuration()
//  private val fs = FileSystem.getLocal(conf)
//
//  override fun afterTest(testCase: TestCase, result: TestResult) {
//    if (fs.exists(path))
//      fs.delete(path, false)
//  }
//
//  override fun beforeTest(testCase: TestCase) {
//    if (fs.exists(path))
//      fs.delete(path, false)
//  }
//
//  init {
//
//    test("Read/write timestamps") {
//
//      val schema = StructType(
//          StructField("a", StringType),
//          StructField("b", TimestampMillisType)
//      )
//
//      val struct = Struct(schema, "hello world", Timestamp.valueOf(LocalDateTime.of(1979, 9, 10, 2, 3, 4)))
//
//      val messageType = ToParquetSchema.toMessageType(schema, "mystruct")
//
//      val writer = parquetWriter(path, conf, messageType)
//      writer.write(struct)
//      writer.close()
//
//      val reader = parquetReader(path, conf)
//      reader.read() shouldBe struct
//      reader.close()
//    }
//
//    test("Read/write times") {
//
//      val schema = StructType(
//          StructField("a", StringType),
//          StructField("b", TimeMillisType)
//      )
//
//      val struct = Struct(schema, "hello world", TimeMillis(12312321))
//
//      val messageType = ToParquetSchema.toMessageType(schema, "mystruct")
//
//      val writer = parquetWriter(path, conf, messageType)
//      writer.write(struct)
//      writer.close()
//
//      val reader = parquetReader(path, conf)
//      reader.read() shouldBe struct
//      reader.close()
//    }
//
//    test("Read/write dates") {
//
//      val schema = StructType(
//          StructField("a", StringType),
//          StructField("b", DateType)
//      )
//
//      val struct = Struct(schema, "hello world", LocalDate.of(1979, 10, 9))
//
//      val messageType = ToParquetSchema.toMessageType(schema, "mystruct")
//
//      val writer = parquetWriter(path, conf, messageType)
//      writer.write(struct)
//      writer.close()
//
//      val reader = parquetReader(path, conf)
//      reader.read() shouldBe struct
//      reader.close()
//    }
//
//    test("Read/write basic types") {
//
//      val schema = StructType(
//          StructField("a", StringType),
//          StructField("b", BooleanType),
//          StructField("v", Float64Type),
//          StructField("d", Float32Type),
//          StructField("e", Int64Type),
//          StructField("f", Int32Type),
//          StructField("g", Int16Type),
//          StructField("h", Int8Type)
//      )
//
//      val struct = Struct(schema, "hello world", true, 123.45, 123.45F, 123L, 423, 87, 14)
//
//      val messageType = ToParquetSchema.toMessageType(schema, "mystruct")
//
//      val writer = parquetWriter(path, conf, messageType)
//      writer.write(struct)
//      writer.close()
//
//      val reader = parquetReader(path, conf)
//      reader.read() shouldBe struct
//      reader.close()
//    }
//
//    test("Read/write enums") {
//
//      val schema = StructType(
//          StructField("a", EnumType(listOf("malbec", "shiraz")))
//      )
//
//      val struct = Struct(schema, "malbec")
//      val messageType = ToParquetSchema.toMessageType(schema, "mystruct")
//
//      val writer = parquetWriter(path, conf, messageType)
//      writer.write(struct)
//      writer.close()
//
//      // enums will lose the values in parquet
//      val reader = parquetReader(path, conf)
//      reader.read() shouldBe Struct(StructType(StructField("a", EnumType())), "malbec")
//      reader.close()
//    }
//  }
//}
