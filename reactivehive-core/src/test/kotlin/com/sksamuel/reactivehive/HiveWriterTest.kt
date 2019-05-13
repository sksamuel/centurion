package com.sksamuel.reactivehive

import arrow.core.Try
import com.sksamuel.reactivehive.formats.ParquetFormat
import com.sksamuel.reactivehive.partitioners.DynamicPartitioner
import io.kotlintest.matchers.collections.shouldBeEmpty
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema

class HiveWriterTest : FunSpec(), HiveTestConfig {

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

    try {
      client.createDatabase(Database("sink_test", null, "/user/hive/warehouse/sink_test", emptyMap()))
    } catch (t: Throwable) {

    }

    test("write to a non partitioned table") {

      val writer = HiveWriter(
          DatabaseName("default"),
          TableName("employees"),
          WriteMode.Overwrite,
          createConfig = CreateTableConfig(schema, null, TableType.MANAGED_TABLE, ParquetFormat),
          client = client,
          fs = fs
      )
      writer.write(users)
      writer.close()

      HiveUtils(client).table(DatabaseName("default"), TableName("employees")).sd.cols shouldBe listOf(
          FieldSchema("name", "string", null),
          FieldSchema("title", "string", null),
          FieldSchema("salary", "double", null),
          FieldSchema("employed", "boolean", null)
      )

      HiveUtils(client).table(DatabaseName("default"), TableName("employees")).partitionKeys.shouldBeEmpty()
    }

    test("write to a partitioned table") {
      val writer = HiveWriter(
          DatabaseName("default"),
          TableName("employees"),
          WriteMode.Overwrite,
          DynamicPartitioner,
          OptimisticFileManager(ReactiveHiveFileNamer),
          createConfig = CreateTableConfig(schema, PartitionPlan(PartitionKey("title"))),
          client = client,
          fs = fs
      )
      writer.write(users)
      writer.close()

      HiveUtils(client).table(DatabaseName("default"), TableName("employees")).sd.cols shouldBe listOf(
          FieldSchema("name", "string", null),
          FieldSchema("salary", "double", null),
          FieldSchema("employed", "boolean", null)
      )

      HiveUtils(client).table(DatabaseName("default"), TableName("employees")).partitionKeys shouldBe listOf(
          FieldSchema("title", "string", null)
      )
    }

    test("create new partitions in the metastore when using dynamic partitions") {

      Try {
        client.dropTable("default", "employees3")
      }

      fun partitions() = client.listPartitions("default", "employees3", Short.MAX_VALUE)

      val createConfig = CreateTableConfig(
          schema,
          PartitionPlan(PartitionKey("title")),
          TableType.MANAGED_TABLE,
          ParquetFormat
      )

      val writer = HiveWriter(
          DatabaseName("default"),
          TableName("employees3"),
          WriteMode.Overwrite,
          DynamicPartitioner,
          OptimisticFileManager(ReactiveHiveFileNamer),
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

    test("setting table type of new tables") {

      for (tableType in listOf(TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE)) {
        Try {
          client.dropTable("default", "employees4")
        }

        val createConfig = CreateTableConfig(schema, null, tableType, location = Path("/user/hive/warehouse/employees4"))

        val writer = HiveWriter(
            DatabaseName("default"),
            TableName("employees4"),
            WriteMode.Overwrite,
            DynamicPartitioner,
            OptimisticFileManager(ReactiveHiveFileNamer),
            createConfig = createConfig,
            client = client,
            fs = fs
        )

        writer.write(users)
        writer.close()

        client.getTable("default", "employees4").tableType shouldBe tableType.asString()
      }
    }

//    it should "not include partition keys in the general columns" in {
//
//      val schema = StructDataType(
//          StructField("a", StringDataType),
//          StructField("b", StringDataType),
//          StructField("c", DoubleDataType),
//          StructField("d", BooleanDataType)
//      )
//
//      val struct = Struct(schema, Seq("x", "y", 1.0, true))
//      val partitionFields = Seq(PartitionField("a"), PartitionField("d"))
//
//      val config = HiveSinkConfig(
//          createTable = true,
//          overwriteTable = true,
//          tableType = TableType.MANAGED_TABLE,
//          partitions = partitionFields
//      )
//
//      Await.ready(
//          Source.single(struct).runWith(com.sksamuel.reactive.scoop.hive.sink(DatabaseName("sink_test"),
//              TableName("woo"),
//              config)),
//          Duration.Inf
//      )
//
//      val table = client.getTable("sink_test", "woo")
//      table.getPartitionKeys.asScala.map(_.getName) shouldBe Seq("a", "d")
//      table.getSd.getCols.asScala.map(_.getName) shouldBe Seq("b", "c")
//    }
//
//    it should "throw an exception if a partition doesn't exist with strict partitioning" in {
//
//      val schema = StructDataType(
//          StructField("a", StringDataType),
//          StructField("b", StringDataType),
//          StructField("c", DoubleDataType),
//          StructField("d", BooleanDataType)
//      )
//
//      val struct = Struct(schema, Seq("x", "y", 1.0, true))
//      val partitionFields = Seq(PartitionField("a"), PartitionField("d"))
//
//      val config = HiveSinkConfig(
//          createTable = true,
//          overwriteTable = true,
//          tableType = TableType.MANAGED_TABLE,
//          partitions = partitionFields,
//          partitioningPolicy = StrictPartitioning
//      )
//
//      Await.ready(
//          Source.single(struct).runWith(com.sksamuel.reactive.scoop.hive.sink(DatabaseName("sink_test"),
//              TableName("woo"),
//              config)),
//          Duration.Inf
//      )
//    }
//
//    it should "support writing structs" in {
//
//      val config = HiveSinkConfig(
//          createTable = true,
//          overwriteTable = true,
//          tableType = TableType.MANAGED_TABLE
//      )
//
//      val schema = StructDataType(
//          StructField("a", StringDataType),
//          StructField("b",
//              StructDataType(
//                  StructField("ba", StringDataType),
//                  StructField("bb", StringDataType)
//              )
//          )
//      )
//      val structs = List(
//          Struct(schema, Seq("foo", Seq("woo", "goo"))),
//          Struct(schema, Seq("moo", Seq("voo", "roo")))
//      )
//
//      Await.ready(
//          Source(structs).runWith(com.sksamuel.reactive.scoop.hive.sink(DatabaseName("sink_test"),
//              TableName("woo"),
//              config)),
//          Duration.Inf
//      )
//
//      val table = client.getTable("sink_test", "woo")
//
//      val data = Await.result(com.sksamuel.reactive.scoop.parquet.source(new Path (table.getSd.getLocation)).runWith(
//          Sink.seq), Duration.Inf)
//
//      data.map(_.values).toSet shouldBe Set(
//          List("foo", List("woo", "goo")),
//          List("moo", List("voo", "roo"))
//      )
//    }
//

  }
}