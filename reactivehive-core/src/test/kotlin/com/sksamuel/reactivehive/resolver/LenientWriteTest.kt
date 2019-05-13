package com.sksamuel.reactivehive.resolver

import arrow.core.Try
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.ConstantFileNamer
import com.sksamuel.reactivehive.CreateTableConfig
import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.HiveTestConfig
import com.sksamuel.reactivehive.HiveUtils
import com.sksamuel.reactivehive.HiveWriter
import com.sksamuel.reactivehive.OptimisticFileManager
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TableName
import com.sksamuel.reactivehive.WriteMode
import com.sksamuel.reactivehive.evolution.NoopSchemaEvolver
import com.sksamuel.reactivehive.formats.ParquetFormat
import com.sksamuel.reactivehive.parquet.parquetReader
import com.sksamuel.reactivehive.parquet.readAll
import com.sksamuel.reactivehive.partitioners.StaticPartitioner
import io.kotlintest.matchers.collections.shouldBeEmpty
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema

class LenientWriteTest : FunSpec() {

  init {

    Try {
      HiveTestConfig.client.dropDatabase("tests")
    }

    Try {
      val db = Database("tests", "test database", "/user/hive/warehouse/tests", emptyMap())
      HiveTestConfig.client.createDatabase(db)
    }

    test("writing data with lenient mode should align structs") {

      val schema = StructType(
          StructField("name", StringType),
          StructField("title", StringType),
          StructField("salary", Float64Type),
          StructField("employed", BooleanType)
      )

      val users = listOf(
          Struct(schema, "sam", "mr", 100.43, false),
          Struct(schema, null, "mr", 230.523, false),
          Struct(schema, "tom", "mr", 60.98, true),
          Struct(schema, "laura", "ms", null, true),
          Struct(schema, "kelly", "ms", 925.162, null)
      )

      val writer = HiveWriter(
          DatabaseName("tests"),
          TableName("lenientdata"),
          WriteMode.Overwrite,
          fileManager = OptimisticFileManager(ConstantFileNamer("test.pq")),
          createConfig = CreateTableConfig(schema, null, TableType.MANAGED_TABLE, ParquetFormat, null),
          evolver = NoopSchemaEvolver,
          partitioner = StaticPartitioner,
          resolver = LenientStructResolver,
          client = HiveTestConfig.client,
          fs = HiveTestConfig.fs
      )
      writer.write(users)
      writer.close()

      val table = HiveUtils(HiveTestConfig.client).table(DatabaseName("tests"), TableName("employees"))

      table.sd.cols shouldBe listOf(
          FieldSchema("name", "string", null),
          FieldSchema("title", "string", null),
          FieldSchema("salary", "double", null),
          FieldSchema("employed", "boolean", null)
      )

      HiveUtils(HiveTestConfig.client).table(DatabaseName("tests"), TableName("lenientdata")).partitionKeys.shouldBeEmpty()

      val file = Path(table.sd.location, "test.pq")
      val reader = parquetReader(file, HiveTestConfig.conf)
      val struct = reader.readAll().toList()
      struct.first().schema shouldBe StructType(
          StructField(name = "name", type = StringType, nullable = true),
          StructField(name = "title", type = StringType, nullable = true),
          StructField(name = "salary", type = Float64Type, nullable = true),
          StructField(name = "employed", type = BooleanType, nullable = true)
      )
      struct.map { it.values }.toList() shouldBe listOf(
          listOf("sam", "mr", 100.43, false),
          listOf(null, "mr", 230.523, false),
          listOf("tom", "mr", 60.98, true),
          listOf("laura", "ms", null, true),
          listOf("kelly", "ms", 925.162, null)
      )
    }
  }
}