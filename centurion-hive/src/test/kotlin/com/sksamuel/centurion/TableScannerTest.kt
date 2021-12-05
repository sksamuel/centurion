package com.sksamuel.centurion

import arrow.core.Try
import com.sksamuel.centurion.HiveTestConfig.client
import com.sksamuel.centurion.HiveTestConfig.fs
import com.sksamuel.centurion.partitioners.DynamicPartitioner
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
        StructField("country", StringType),
        StructField("continent", StringType)
    )

    val locations = listOf(
        Struct(schema, "chicago", "us", "na"),
        Struct(schema, "atlanta", "us", "na"),
        Struct(schema, "london", "uk", "eu"),
        Struct(schema, "athens", "gr", "eu"),
        Struct(schema, "sacramento", "us", "na"),
        Struct(schema, "amsterdam", "nl", "eu"),
        Struct(schema, "manchester", "uk", "eu")
    )

    test("partition pushdown should filter files for a table with a single partition") {

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

    test("partition pushdown should filter files for a table with multiple partitions") {

      val writer = HiveWriter.fromKotlin(
          DatabaseName("tests"),
          TableName("pp"),
          WriteMode.Overwrite,
          partitioner = DynamicPartitioner,
          fileManager = OptimisticFileManager(ConstantFileNamer("locations.pq")),
          createConfig = CreateTableConfig.fromKotlin(schema, PartitionPlan(PartitionKey("continent"), PartitionKey("country"))),
          client = client,
          fs = fs
      )
      writer.write(locations)
      writer.close()

      val scanner = TableScanner(client, fs)
      scanner.scan(DatabaseName("tests"), TableName("pp"), PartitionPushdown.eq("country", "us")) shouldBe listOf(
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/continent=na/country=us/locations.pq")
      )
      scanner.scan(DatabaseName("tests"), TableName("pp"), PartitionPushdown.eq("continent", "na")) shouldBe listOf(
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/continent=na/country=us/locations.pq")
      )
      scanner.scan(DatabaseName("tests"), TableName("pp"), PartitionPushdown.eq("continent", "eu")).toSet() shouldBe setOf(
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/continent=eu/country=uk/locations.pq"),
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/continent=eu/country=nl/locations.pq"),
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/continent=eu/country=gr/locations.pq")
      )
      scanner.scan(DatabaseName("tests"), TableName("pp"), null).toSet() shouldBe setOf(
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/continent=na/country=us/locations.pq"),
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/continent=eu/country=uk/locations.pq"),
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/continent=eu/country=nl/locations.pq"),
          Path("hdfs://namenode:8020/user/hive/warehouse/tests/pp/continent=eu/country=gr/locations.pq")
      )
    }

  }
}
