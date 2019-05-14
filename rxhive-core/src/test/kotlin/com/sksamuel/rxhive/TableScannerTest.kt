package com.sksamuel.rxhive

import arrow.core.Try
import com.sksamuel.rxhive.HiveTestConfig.client
import com.sksamuel.rxhive.HiveTestConfig.fs
import com.sksamuel.rxhive.partitioners.DynamicPartitioner
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.Database

class TableScannerTest : FunSpec() {

  init {

    Try {
      client.dropDatabase("tests")
    }

    Try {
      client.createDatabase(Database("tests", null, "/user/hive/warehouse/tests", emptyMap()))
    }

    val schema = StructType(
        StructField("city", StringType),
        StructField("country", StringType)
    )

    val locations = listOf(
        Struct(schema, "chicago", "us"),
        Struct(schema, "atlanta", "us"),
        Struct(schema, "london", "uk"),
        Struct(schema, "athens", "gr"),
        Struct(schema, "sacramento", "us"),
        Struct(schema, "amsterdam", "nl"),
        Struct(schema, "manchester", "uk")
    )

    test("partition pushdown should filter files for a single partition table") {

      val writer = HiveWriter.fromKotlin(
          DatabaseName("tests"),
          TableName("pp"),
          WriteMode.Overwrite,
          partitioner = DynamicPartitioner,
          fileManager = OptimisticFileManager(ConstantFileNamer("locations.pq")),
          createConfig = CreateTableConfig.fromKotlin(schema, PartitionPlan(PartitionKey("country"))),
          client = client,
          fs = fs
      )
      writer.write(locations)
      writer.close()

      val scanner = TableScanner(client, fs)
      scanner.scan(DatabaseName("tests"), TableName("pp"), PartitionPushdown.eq("country", "us")) shouldBe listOf(
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/country=us/locations.pq")
      )
      scanner.scan(DatabaseName("tests"), TableName("pp"), null).toSet() shouldBe setOf(
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/country=us/locations.pq"),
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/country=gr/locations.pq"),
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/country=nl/locations.pq"),
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/country=uk/locations.pq")
      )
    }
  }

}