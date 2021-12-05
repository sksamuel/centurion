package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.nullable
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import java.nio.file.Paths

class SparkCompatibilityTest : FunSpec() {
  init {

    val conf = SparkConf()
      .setMaster("local")
      .setAppName("centurion")
      .set("spark.cores.max", "2")
    val sc = SparkContext(conf)

    val spark = SparkSession(sc)

    test("centurion should be able to read spark parquet output") {
      data class People(val name: String, val location: String)

      val df = spark.createDataFrame(
        listOf(
          People("Clint Eastwood", "Hollywood"),
          People("Patrick Stewart", "Yorkshire"),
        ),
        People::class.java
      )

      df.repartition(1)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet("file:////tmp/spark.parquet")

      val file = Paths.get("/tmp/spark.parquet")
      val structs = Parquet.reader(file.toFile(), Configuration()).sequence().toList()
      structs.first().schema shouldBe Schema.Struct(
        "spark_schema",
        Schema.Field("location", Schema.Strings.nullable()),
        Schema.Field("name", Schema.Strings.nullable()),
      )
      structs.first().values shouldBe listOf("Hollywood", "Clint Eastwood")
      structs[1].values shouldBe listOf("Yorkshire", "Patrick Stewart")
    }

    test("centurion should be able to read spark parquet arrays") {
      data class Movie(val name: String, val year: Int, val actors: List<String>)

      val df = spark.createDataFrame(
        listOf(
          Movie("Gladiator", 2000, listOf("Russell Crowe", "Oliver Reed")),
          Movie("Wrath of Khan", 1982, listOf("William Shatner", "Ricardo Montalbán", "Leonard Nimoy"))
        ),
        Movie::class.java
      )

      df.repartition(1)
        .write()
        .mode(SaveMode.Overwrite)
        .parquet("file:////tmp/spark.parquet")

      val file = Paths.get("/tmp/spark.parquet")
      val structs = Parquet.reader(file.toFile(), Configuration()).sequence().toList()
      structs.first().schema shouldBe Schema.Struct(
        "spark_schema",
        Schema.Field("actors", Schema.Array(Schema.Strings.nullable()).nullable()),
        Schema.Field("name", Schema.Strings.nullable()),
        Schema.Field("year", Schema.Int32),
      )
    }
  }
}

