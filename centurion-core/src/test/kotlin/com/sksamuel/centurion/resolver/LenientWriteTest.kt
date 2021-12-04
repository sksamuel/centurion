package com.sksamuel.centurion.resolver

import arrow.core.Try
import com.sksamuel.centurion.BooleanType
import com.sksamuel.centurion.ConstantFileNamer
import com.sksamuel.centurion.CreateTableConfig
import com.sksamuel.centurion.DatabaseName
import com.sksamuel.centurion.Float64Type
import com.sksamuel.centurion.HiveTestConfig
import com.sksamuel.centurion.HiveTestConfig.client
import com.sksamuel.centurion.HiveTestConfig.fs
import com.sksamuel.centurion.HiveUtils
import com.sksamuel.centurion.HiveWriter
import com.sksamuel.centurion.OptimisticFileManager
import com.sksamuel.centurion.StringType
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructField
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.TableName
import com.sksamuel.centurion.WriteMode
import com.sksamuel.centurion.evolution.NoopSchemaEvolver
import com.sksamuel.centurion.formats.ParquetFormat
import com.sksamuel.centurion.parquet.parquetReader
import com.sksamuel.centurion.parquet.readAll
import com.sksamuel.centurion.partitioners.StaticPartitioner
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

      Try {
        HiveTestConfig.client.dropTable("tests", "lenientdata", true, true)
      }

      val metastoreSchema = StructType(
          StructField("name", StringType),
          StructField("title", StringType),
          StructField("salary", Float64Type),
          StructField("employed", BooleanType)
      )

      val u1 = Struct(
          StructType(
              StructField("name", StringType),
              StructField("title", StringType),
              StructField("employed", BooleanType)
          ),
          listOf("sam", "mr", true)
      )

      val u2 = Struct(
          StructType(
              StructField("name", StringType),
              StructField("salary", Float64Type),
              StructField("employed", BooleanType)
          ),
          listOf("bob", 3485.63, false)
      )

      val writer = HiveWriter(
          DatabaseName("tests"),
          TableName("lenientdata"),
          WriteMode.Overwrite,
          fileManager = OptimisticFileManager(ConstantFileNamer("test.pq")),
          createConfig = CreateTableConfig(metastoreSchema, null, TableType.MANAGED_TABLE, ParquetFormat, null),
          evolver = NoopSchemaEvolver,
          partitioner = StaticPartitioner,
          resolver = LenientStructResolver,
          client = HiveTestConfig.client,
          fs = HiveTestConfig.fs
      )
      writer.write(listOf(u1, u2))
      writer.close()

      val table = HiveUtils(client, fs).table(DatabaseName("tests"), TableName("lenientdata"))

      table.sd.cols shouldBe listOf(
          FieldSchema("name", "string", null),
          FieldSchema("title", "string", null),
          FieldSchema("salary", "double", null),
          FieldSchema("employed", "boolean", null)
      )

      HiveUtils(client, fs).table(DatabaseName("tests"), TableName("lenientdata")).partitionKeys.shouldBeEmpty()

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
          listOf("sam", "mr", null, true),
          listOf("bob", null, 3485.63, false)
      )
    }
  }
}
