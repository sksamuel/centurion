package com.sksamuel.reactivehive

import io.kotlintest.specs.FunSpec
import io.kotlintest.shouldBe

class ParquetFormatTest : FunSpec() {

  init {
    test("should set correct serde information") {
      ParquetFormat.serde().inputFormat shouldBe "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
      ParquetFormat.serde().outputFormat shouldBe "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
      ParquetFormat.serde().serializationLib shouldBe "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
  }

}