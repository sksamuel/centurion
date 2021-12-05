package com.sksamuel.centurion

import arrow.core.Try
import com.sksamuel.centurion.HiveTestConfig.client
import com.sksamuel.centurion.HiveTestConfig.fs
import com.sksamuel.centurion.evolution.NoopSchemaEvolver
import com.sksamuel.centurion.formats.ParquetFormat
import com.sksamuel.centurion.partitioners.DynamicPartitioner
import com.sksamuel.centurion.resolver.LenientStructResolver
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.Database

class DynamicPartitionerTest : FunSpec() {

  val schema = StructType(
      StructField("name", StringType),
      StructField("title", StringType),
      StructField("salary", Float64Type),
      StructField("employed", BooleanType)
  )

  val users = listOf(
      Struct(schema, "sam", "mr", 100.43, false),
      Struct(schema, "ben", "mr", 230.523, false),
      Struct(schema, "tom", "mr", 60.98, true),
      Struct(schema, "laura", "ms", 421.512, true),
      Struct(schema, "kelly", "ms", 925.162, false)
  )

  init {

    Try {
      client.createDatabase(Database("tests", null, "/user/hive/warehouse/tests", emptyMap()))
    }

    test("create new partitions in the metastore") {

      Try {
        client.dropTable("tests", "employees3")
      }

      fun partitions() = client.listPartitions("tests", "employees3", Short.MAX_VALUE)

      val createConfig = CreateTableConfig(
          schema,
          PartitionPlan(PartitionKey("title")),
          TableType.MANAGED_TABLE,
          ParquetFormat,
          null
      )

      val writer = HiveWriter(
          DatabaseName("tests"),
          TableName("employees3"),
          WriteMode.Overwrite,
          partitioner = DynamicPartitioner,
          fileManager = OptimisticFileManager(RxHiveFileNamer),
          evolver = NoopSchemaEvolver,
          resolver = LenientStructResolver,
          createConfig = createConfig,
          client = client,
          fs = fs
      )

      writer.write(users)
      writer.close()

      partitions().any {
        it.values == listOf("mr")
      } shouldBe true

      partitions().any {
        it.values == listOf("ms")
      } shouldBe true

      partitions().any {
        it.values == listOf("qq")
      } shouldBe false
    }
  }
}
