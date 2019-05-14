package com.sksamuel.rxhive

import com.sksamuel.rxhive.HiveTestConfig.client
import com.sksamuel.rxhive.HiveTestConfig.fs
import io.kotlintest.matchers.collections.shouldBeEmpty
import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.matchers.collections.shouldNotBeEmpty
import io.kotlintest.specs.FunSpec

class HiveUtilsTest : FunSpec() {

  private val utils = HiveUtils(client, fs)

  private val schema = StructType(
      StructField("name", StringType),
      StructField("title", StringType),
      StructField("salary", Float64Type),
      StructField("employed", BooleanType)
  )

  private val users = listOf(
      Struct(schema, "sam", "mr", 100.43, false),
      Struct(schema, "ben", "mr", 230.523, false),
      Struct(schema, "tom", "mr", 60.98, true),
      Struct(schema, "laura", "ms", 421.512, true),
      Struct(schema, "kelly", "ms", 925.162, false)
  )

  init {
    test("list databases") {
      utils.listDatabases().shouldContain(DatabaseName("default"))
    }
    test("truncate table") {
      val writer = HiveWriter.fromKotlin(
          DatabaseName("tests"),
          TableName("wibble"),
          createConfig = CreateTableConfig.fromKotlin(schema),
          client = client,
          fs = fs
      )
      writer.write(users)
      writer.close()

      val scanner = TableScanner(client, fs)
      scanner.scan(DatabaseName("tests"), TableName("wibble"), null).shouldNotBeEmpty()
      utils.truncateTable(DatabaseName("tests"), TableName("wibble"))
      scanner.scan(DatabaseName("tests"), TableName("wibble"), null).shouldBeEmpty()
    }
  }
}