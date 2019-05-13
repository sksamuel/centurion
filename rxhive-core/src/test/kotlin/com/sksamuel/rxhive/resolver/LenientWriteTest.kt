package com.sksamuel.rxhive.resolver

import arrow.core.Try
import com.sksamuel.rxhive.BooleanType
import com.sksamuel.rxhive.ConstantFileNamer
import com.sksamuel.rxhive.CreateTableConfig
import com.sksamuel.rxhive.DatabaseName
import com.sksamuel.rxhive.Float64Type
import com.sksamuel.rxhive.HiveTestConfig
import com.sksamuel.rxhive.HiveUtils
import com.sksamuel.rxhive.HiveWriter
import com.sksamuel.rxhive.OptimisticFileManager
import com.sksamuel.rxhive.StringType
import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructField
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TableName
import com.sksamuel.rxhive.WriteMode
import com.sksamuel.rxhive.evolution.NoopSchemaEvolver
import com.sksamuel.rxhive.formats.ParquetFormat
import com.sksamuel.rxhive.parquet.parquetReader
import com.sksamuel.rxhive.parquet.readAll
import com.sksamuel.rxhive.partitioners.StaticPartitioner
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

      val table = HiveUtils(HiveTestConfig.client).table(DatabaseName("tests"), TableName("lenientdata"))

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
          listOf("sam", "mr", null, true),
          listOf("bob", null, 3485.63, false)
      )
    }
  }
}