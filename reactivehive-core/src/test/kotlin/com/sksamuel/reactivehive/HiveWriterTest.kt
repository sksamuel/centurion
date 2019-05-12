package com.sksamuel.reactivehive

import com.sksamuel.reactivehive.formats.ParquetFormat
import io.kotlintest.matchers.collections.shouldBeEmpty
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
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
          schema,
          ReactiveHiveFileNamer,
          WriteMode.Overwrite,
          TableType.MANAGED_TABLE,
          null,
          ParquetFormat,
          DefaultPartitionLocator,
          client,
          fs
      )
      writer.write(users)

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
          schema,
          ReactiveHiveFileNamer,
          WriteMode.Overwrite,
          TableType.MANAGED_TABLE,
          PartitionPlan(PartitionKey("title")),
          ParquetFormat,
          DefaultPartitionLocator,
          client,
          fs
      )
      writer.write(users)

      HiveUtils(client).table(DatabaseName("default"), TableName("employees")).sd.cols shouldBe listOf(
          FieldSchema("name", "string", null),
          FieldSchema("salary", "double", null),
          FieldSchema("employed", "boolean", null)
      )

      HiveUtils(client).table(DatabaseName("default"), TableName("employees")).partitionKeys shouldBe listOf(
          FieldSchema("title", "string", null)
      )
    }
//
//    it should "create new partitions in the metastore when using dynamic partitions" in {
//
//      Try {
//        client.dropTable("sink_test", "employees3")
//      }
//
//      def partitions = client . listPartitions ("sink_test", "employees3", Short.MaxValue).asScala
//
//      val config = HiveSinkConfig(
//          createTable = true,
//          overwriteTable = true,
//          partitions = Seq(PartitionField("title")),
//          partitioningPolicy = new DynamicPartitioning ()
//      )
//
//      Await.ready(
//          Source(users).runWith(com.sksamuel.reactive.scoop.hive.sink(DatabaseName("sink_test"),
//              TableName("employees3"),
//              config)),
//          Duration.Inf
//      )
//
//      partitions.exists {
//        partition =>
//        partition.getValues.asScala.toList == List("mr")
//      } shouldBe true
//
//      partitions.exists {
//        partition =>
//        partition.getValues.asScala.toList == List("ms")
//      } shouldBe true
//
//      partitions.exists {
//        partition =>
//        partition.getValues.asScala.toList == List("qq")
//      } shouldBe false
//    }
//
//    it should "allow setting table type of new tables" in {
//      val config1 = HiveSinkConfig(
//          createTable = true,
//          overwriteTable = true,
//          tableType = TableType.MANAGED_TABLE
//      )
//
//      Await.ready(
//          Source(users).runWith(com.sksamuel.reactive.scoop.hive.sink(DatabaseName("sink_test"),
//              TableName("abc"),
//              config1)),
//          Duration.Inf
//      )
//      client.getTable("sink_test", "abc").getTableType shouldBe "MANAGED_TABLE"
//
//      val config2 = HiveSinkConfig(
//          createTable = true,
//          overwriteTable = true,
//          tableType = TableType.EXTERNAL_TABLE
//      )
//      Await.ready(
//          Source(users).runWith(com.sksamuel.reactive.scoop.hive.sink(DatabaseName("sink_test"),
//              TableName("abc"),
//              config2)),
//          Duration.Inf
//      )
//
//      client.getTable("sink_test", "abc").getTableType shouldBe "EXTERNAL_TABLE"
//    }
//
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
//    it should "return the count of records written" in {
//
//      val config = HiveSinkConfig(
//          createTable = true,
//          overwriteTable = true,
//          tableType = TableType.MANAGED_TABLE
//      )
//
//      val schema = StructDataType(StructField("a", StringDataType))
//      val struct = Struct(schema, Seq("foo"))
//      val f = Source(List.fill(99)(struct))
//          .runWith(com.sksamuel.reactive.scoop.hive.sink(DatabaseName("sink_test"), TableName("woo"), config))
//      Await.result(f, Duration.Inf) shouldBe 99
//    }
  }
}