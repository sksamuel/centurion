package com.sksamuel.reactivehive

import arrow.core.Try
import com.sksamuel.reactivehive.HiveTestConfig.client
import com.sksamuel.reactivehive.HiveTestConfig.fs
import com.sksamuel.reactivehive.evolution.NoopSchemaEvolver
import com.sksamuel.reactivehive.formats.ParquetFormat
import com.sksamuel.reactivehive.partitioners.StaticPartitioner
import com.sksamuel.reactivehive.resolver.LenientStructResolver
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