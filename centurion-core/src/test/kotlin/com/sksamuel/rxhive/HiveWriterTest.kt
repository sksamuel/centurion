package com.sksamuel.rxhive

import arrow.core.Try
import com.sksamuel.rxhive.HiveTestConfig.client
import com.sksamuel.rxhive.HiveTestConfig.conf
import com.sksamuel.rxhive.HiveTestConfig.fs
import com.sksamuel.rxhive.evolution.NoopSchemaEvolver
import com.sksamuel.rxhive.formats.ParquetFormat
import com.sksamuel.rxhive.parquet.parquetReader
import com.sksamuel.rxhive.parquet.readAll
import com.sksamuel.rxhive.partitioners.DynamicPartitioner
import com.sksamuel.rxhive.partitioners.StaticPartitioner
import com.sksamuel.rxhive.resolver.LenientStructResolver
import io.kotlintest.matchers.collections.shouldBeEmpty
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.metastore.api.FieldSchema

class HiveWriterTest : FunSpec() {

  private fun partitions(name: String) = client.listPartitions("tests", name, Short.MAX_VALUE)

  init {

    Try {
      client.dropDatabase("tests")
    }

    Try {
      val db = Database("tests", "test database", "/user/hive/warehouse/tests", emptyMap())
      client.createDatabase(db)
    }

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

    test("write to a non partitioned table") {

      val writer = HiveWriter(
          DatabaseName("tests"),
          TableName("employees"),
          WriteMode.Overwrite,
          partitioner = StaticPartitioner,
          evolver = NoopSchemaEvolver,
          resolver = LenientStructResolver,
          fileManager = OptimisticFileManager(ConstantFileNamer("test.pq")),
          createConfig = CreateTableConfig(schema, null, TableType.MANAGED_TABLE, ParquetFormat, null),
          client = client,
          fs = fs
      )
      writer.write(users)
      writer.close()

      val table = HiveUtils(client, fs).table(DatabaseName("tests"), TableName("employees"))

      table.sd.cols shouldBe listOf(
          FieldSchema("name", "string", null),
          FieldSchema("title", "string", null),
          FieldSchema("salary", "double", null),
          FieldSchema("employed", "boolean", null)
      )

      HiveUtils(client, fs).table(DatabaseName("tests"), TableName("employees")).partitionKeys.shouldBeEmpty()

      val file = Path(table.sd.location, "test.pq")
      val reader = parquetReader(file, conf)
      val struct = reader.readAll().toList()
      struct.first().schema shouldBe StructType(
          StructField(name = "name", type = StringType, nullable = true),
          StructField(name = "title", type = StringType, nullable = true),
          StructField(name = "salary", type = Float64Type, nullable = true),
          StructField(name = "employed", type = BooleanType, nullable = true)
      )
      struct.map { it.values }.toList() shouldBe listOf(
          listOf("sam", "mr", 100.43, false),
          listOf("ben", "mr", 230.523, false),
          listOf("tom", "mr", 60.98, true),
          listOf("laura", "ms", 421.512, true),
          listOf("kelly", "ms", 925.162, false)
      )
    }

    test("write to a partitioned table") {

      val writer = HiveWriter(
          DatabaseName("tests"),
          TableName("partitionedtest"),
          WriteMode.Overwrite,
          DynamicPartitioner,
          OptimisticFileManager(ConstantFileNamer("test.pq")),
          evolver = NoopSchemaEvolver,
          resolver = LenientStructResolver,
          createConfig = CreateTableConfig(schema,
              PartitionPlan(PartitionKey("title")),
              TableType.MANAGED_TABLE,
              ParquetFormat,
              null),
          client = client,
          fs = fs
      )
      writer.write(users)
      writer.close()

      HiveUtils(client, fs).table(DatabaseName("tests"), TableName("partitionedtest")).sd.cols shouldBe listOf(
          FieldSchema("name", "string", null),
          FieldSchema("salary", "double", null),
          FieldSchema("employed", "boolean", null)
      )

      HiveUtils(client, fs).table(DatabaseName("tests"), TableName("partitionedtest")).partitionKeys shouldBe listOf(
          FieldSchema("title", "string", null)
      )

      partitions("partitionedtest").map { it.values } shouldBe listOf(listOf("mr"), listOf("ms"))
      partitions("partitionedtest").forEach {
        val file = Path(it.sd.location, "test.pq")
        val reader = parquetReader(file, conf)
        val struct = reader.read()
        struct.schema shouldBe StructType(
            StructField(name = "name", type = StringType, nullable = true),
            StructField(name = "salary", type = Float64Type, nullable = true),
            StructField(name = "employed", type = BooleanType, nullable = true)
        )
      }
    }

    test("setting table type of new tables") {

      for (tableType in listOf(TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE)) {
        Try {
          client.dropTable("tests", "employees4")
        }

        val createConfig = CreateTableConfig(
            schema,
            null,
            tableType,
            ParquetFormat,
            location = Path("/user/hive/warehouse/employees4")
        )

        val writer = HiveWriter(
            DatabaseName("tests"),
            TableName("employees4"),
            WriteMode.Overwrite,
            DynamicPartitioner,
            OptimisticFileManager(RxHiveFileNamer),
            evolver = NoopSchemaEvolver,
            resolver = LenientStructResolver,
            createConfig = createConfig,
            client = client,
            fs = fs
        )

        writer.write(users)
        writer.close()

        client.getTable("tests", "employees4").tableType shouldBe tableType.asString()
      }
    }

    test("align data with the target schema") {

      for (tableType in listOf(TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE)) {
        Try {
          client.dropTable("tests", "aligntest")
        }

        val s2 = StructType(
            StructField("a", StringType),
            StructField("b", BooleanType),
            StructField("c", Int32Type)
        )

        val createConfig = CreateTableConfig(
            s2,
            null,
            tableType,
            ParquetFormat,
            Path("/user/hive/warehouse/aligntest")
        )

        val writer = HiveWriter(
            DatabaseName("tests"),
            TableName("aligntest"),
            WriteMode.Overwrite,
            DynamicPartitioner,
            evolver = NoopSchemaEvolver,
            resolver = LenientStructResolver,
            fileManager = OptimisticFileManager(ConstantFileNamer("test.pq")),
            createConfig = createConfig,
            client = client,
            fs = fs
        )

        val structs = listOf(
            Struct(
                StructType(StructField("b", StringType), StructField("a", BooleanType), StructField("c", Int32Type)),
                true,
                "x",
                1
            ),
            Struct(
                StructType(StructField("a", StringType), StructField("c", BooleanType), StructField("b", Int32Type)),
                "y",
                2,
                false
            ),
            Struct(
                StructType(StructField("c", StringType), StructField("b", BooleanType), StructField("a", Int32Type)),
                3,
                true,
                "z"
            )
        )

        writer.write(structs)
        writer.close()

        val table = client.getTable("tests", "aligntest")
        val file = Path(table.sd.location, "test.pq")
        val reader = parquetReader(file, conf)
        val struct = reader.readAll().toList()
        struct.first().schema shouldBe StructType(
            StructField(name = "a", type = StringType, nullable = true),
            StructField(name = "b", type = BooleanType, nullable = true),
            StructField(name = "c", type = Int32Type, nullable = true)
        )
        struct.map { it.values }.toList() shouldBe listOf(
            listOf("x", true, 1),
            listOf("y", false, 2),
            listOf("z", true, 3)
        )
      }
    }
  }
}