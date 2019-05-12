package com.sksamuel.reactivehive

import com.sksamuel.reactivehive.formats.ParquetFormat
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.FieldSchema

class CreateTableTest : FunSpec(), HiveTestConfig {

  init {

    test("creating a table w/o partitions") {

      try {
        client.dropTable("default", "foo")
      } catch (t: Throwable) {
      }

      val schema = StructType(
          StructField("a", StringType),
          StructField("b", BooleanType),
          StructField("c", Int64Type)
      )

      createTable(
          DatabaseName("default"),
          TableName("foo"),
          CreateTableConfig(schema, PartitionPlan.empty, TableType.MANAGED_TABLE, ParquetFormat),
          client = client,
          fs = fs
      ) shouldNotBe null

      val table = client.getTable("default", "foo")
      table.tableName shouldBe "foo"
      table.sd.cols shouldBe listOf(
          FieldSchema("a", "string", null),
          FieldSchema("b", "boolean", null),
          FieldSchema("c", "bigint", null)
      )
      table.tableType shouldBe "MANAGED_TABLE"
      table.owner shouldBe "hive"
      table.sd.serdeInfo.serializationLib shouldBe ParquetFormat.serde().serializationLib
    }

    test("creating a table with partitions") {

      try {
        client.dropTable("default", "foo")
      } catch (t: Throwable) {
      }

      val schema = StructType(
          StructField("a", StringType),
          StructField("b", BooleanType),
          StructField("c", Int64Type)
      )

      val plan = PartitionPlan(PartitionKey("b"), PartitionKey("c"))

      createTable(
          DatabaseName("default"),
          TableName("foo"),
          CreateTableConfig(schema, plan, TableType.MANAGED_TABLE, ParquetFormat),
          client = client,
          fs = fs
      ) shouldNotBe null

      val table = client.getTable("default", "foo")
      table.tableName shouldBe "foo"
      table.sd.cols shouldBe listOf(
          FieldSchema("a", "string", null)
      )
      table.partitionKeys shouldBe listOf(
          FieldSchema("b", "string", null),
          FieldSchema("c", "string", null)
      )
      table.tableType shouldBe "MANAGED_TABLE"
      table.owner shouldBe "hive"
      table.sd.serdeInfo.serializationLib shouldBe ParquetFormat.serde().serializationLib
    }

    test("create external table with custom location") {

      try {
        client.dropTable("default", "foo")
      } catch (t: Throwable) {
      }

      val schema = StructType(StructField("a", StringType))

      createTable(
          DatabaseName("default"),
          TableName("foo"),
          CreateTableConfig(schema,
              PartitionPlan.empty,
              TableType.EXTERNAL_TABLE,
              ParquetFormat,
              Path("/user/hive/warehouse/wibble")
          ),
          client = client,
          fs = fs
      ) shouldNotBe null

      val table = client.getTable("default", "foo")
      table.sd.location shouldBe "hdfs://localhost:8020/user/hive/warehouse/wibble"
    }
  }
}