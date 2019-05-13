package com.sksamuel.reactivehive

import arrow.core.Try
import com.sksamuel.reactivehive.HiveTestConfig.client
import com.sksamuel.reactivehive.HiveTestConfig.fs
import com.sksamuel.reactivehive.formats.ParquetFormat
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema

class CreateTableTest : FunSpec() {

  init {

    Try {
      client.dropDatabase("tests")
    }

    Try {
      val db = Database("tests", "test database", "/user/hive/warehouse/tests", emptyMap())
      client.createDatabase(db)
    }

    test("creating a table w/o partitions") {

      Try {
        client.dropTable("tests", "foo")
      }

      val schema = StructType(
          StructField("a", StringType),
          StructField("b", BooleanType),
          StructField("c", Int64Type)
      )

      createTable(
          DatabaseName("tests"),
          TableName("foo"),
          CreateTableConfig(schema, PartitionPlan.empty, TableType.MANAGED_TABLE, ParquetFormat, null),
          client = client,
          fs = fs
      ) shouldNotBe null

      val table = client.getTable("tests", "foo")
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

      Try {
        client.dropTable("tests", "foo")
      }

      val schema = StructType(
          StructField("a", StringType),
          StructField("b", BooleanType),
          StructField("c", Int64Type)
      )

      val plan = PartitionPlan(PartitionKey("b"), PartitionKey("c"))

      createTable(
          DatabaseName("tests"),
          TableName("foo"),
          CreateTableConfig(schema, plan, TableType.MANAGED_TABLE, ParquetFormat, null),
          client = client,
          fs = fs
      ) shouldNotBe null

      val table = client.getTable("tests", "foo")
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

      Try {
        client.dropTable("tests", "foo")
      }

      val schema = StructType(StructField("a", StringType))

      createTable(
          DatabaseName("tests"),
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

      val table = client.getTable("tests", "foo")
      table.sd.location shouldBe "hdfs://namenode:8020/user/hive/warehouse/wibble"
    }
  }
}