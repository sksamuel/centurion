package com.sksamuel.rxhive.parquet

import com.sksamuel.rxhive.Float64Type
import com.sksamuel.rxhive.Int32Type
import com.sksamuel.rxhive.StringType
import com.sksamuel.rxhive.StructField
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TimestampMillisType
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class ParquetReaderTest : FunSpec() {

  private val conf: Configuration = Configuration()
  private val fs: FileSystem = FileSystem.getLocal(conf)

  init {

    test("reading an existing parquet file") {

      val path = Path("file://" + this.javaClass.getResource("/userdata.parquet").file)
      val struct = parquetReader(path, conf).read()

      struct.schema shouldBe StructType(
          StructField("registration_dttm", TimestampMillisType, true),
          StructField("id", Int32Type, true),
          StructField("first_name", StringType, true),
          StructField("last_name", StringType, true),
          StructField("email", StringType, true),
          StructField("gender", StringType, true),
          StructField("ip_address", StringType, true),
          StructField("cc", StringType, true),
          StructField("country", StringType, true),
          StructField("birthdate", StringType, true),
          StructField("salary", Float64Type, true),
          StructField("title", StringType, true),
          StructField("comments", StringType, true)
      )

      struct.values.drop(1) shouldBe
          listOf(
              1,
              "Amanda",
              "Jordan",
              "ajordan0@com.com",
              "Female",
              "1.197.201.2",
              "6759521864920116",
              "Indonesia",
              "3/8/1971",
              49756.53,
              "Internal Auditor",
              "1E+02"
          )

//      struct.values shouldBe
//          listOf(
//              Timestamp.valueOf("2016-02-03 13:55:29.0"),
//              1,
//              "Amanda",
//              "Jordan",
//              "ajordan0@com.com",
//              "Female",
//              "1.197.201.2",
//              "6759521864920116",
//              "Indonesia",
//              "3/8/1971",
//              49756.53,
//              "Internal Auditor",
//              "1E+02"
//          )
    }
  }
}
