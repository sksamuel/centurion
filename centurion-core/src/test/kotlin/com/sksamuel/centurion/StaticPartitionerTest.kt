package com.sksamuel.centurion

import arrow.core.Try
import com.sksamuel.centurion.HiveTestConfig.client
import com.sksamuel.centurion.HiveTestConfig.fs
import com.sksamuel.centurion.evolution.NoopSchemaEvolver
import com.sksamuel.centurion.formats.ParquetFormat
import com.sksamuel.centurion.partitioners.StaticPartitioner
import com.sksamuel.centurion.resolver.LenientStructResolver
import io.kotlintest.shouldThrowAny
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.Database

class StaticPartitionerTest : FunSpec() {

  init {

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

    Try {
      client.createDatabase(Database("tests", null, "/user/hive/warehouse/sink_test", emptyMap()))
    }

    test("fail if a partition doesn't exist with static partitioning") {

      Try {
        client.dropTable("tests", "static_test")
      }

      val writer = HiveWriter(
          DatabaseName("tests"),
          TableName("static_test"),
          WriteMode.Overwrite,
          createConfig = CreateTableConfig(schema, PartitionPlan(PartitionKey("title")), TableType.MANAGED_TABLE, ParquetFormat, null),
          fileManager = OptimisticFileManager(),
          evolver = NoopSchemaEvolver,
          resolver = LenientStructResolver,
          partitioner = StaticPartitioner,
          client = client,
          fs = fs
      )

      shouldThrowAny {
        writer.write(users)
      }

      writer.close()
    }
  }
}
