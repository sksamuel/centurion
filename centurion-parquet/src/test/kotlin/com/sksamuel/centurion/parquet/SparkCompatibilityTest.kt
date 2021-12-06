package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.nullable
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.WrappedArray
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
      structs.first().values shouldBe listOf(listOf("Russell Crowe", "Oliver Reed"), "Gladiator", 2000)
      structs[1].values shouldBe listOf(
        listOf("William Shatner", "Ricardo Montalbán", "Leonard Nimoy"),
        "Wrath of Khan",
        1982
      )
    }

    test("spark should be able to read centurion parquet arrays") {
      val schema = Schema.Struct(
        "spark_schema",
        Schema.Field("name", Schema.Strings),
        Schema.Field("year", Schema.Int32),
        Schema.Field("actors", Schema.Array(Schema.Strings)),
      )

      val structs = listOf(
        Struct(schema, listOf("Gladiator", 2000, listOf("Russell Crowe", "Oliver Reed"))),
        Struct(schema, listOf("Wrath of Khan", 1982, listOf("William Shatner", "Ricardo Montalbán", "Leonard Nimoy"))),
      )

      val writer = Parquet.writer(Paths.get("/tmp/centurion.parquet"), Configuration(), schema, ParquetFileWriter.Mode.OVERWRITE)
      writer.write(structs)
      writer.close()

      val df = spark.read().parquet("file:////tmp/centurion.parquet")
      df.count() shouldBe 2

      val names = df.select("name").collectAsList()
      names[0].get(0) shouldBe "Gladiator"
      names[1].get(0) shouldBe "Wrath of Khan"

      val years = df.select("year").collectAsList()
      years[0].get(0) shouldBe 2000
      years[1].get(0) shouldBe 1982

      val actors = df.select("actors").collectAsList()
      (actors[0].get(0) as WrappedArray<String>).array() shouldBe listOf("Russell Crowe", "Oliver Reed")

      (actors[1].get(0) as WrappedArray<String>).array() shouldBe listOf(
        "William Shatner",
        "Ricardo Montalbán",
        "Leonard Nimoy"
      )
    }

    test("spark should be able to read centurion parquet maps") {

      val schema = Schema.Struct(
        "spark_schema",
        Schema.Field("civilization", Schema.Strings),
        Schema.Field("people", Schema.Map(Schema.Strings)),
      )

      val structs = listOf(
        Struct(schema, listOf("Rome", mapOf("Dictator" to "Caesar", "Emperor" to "Augustus", "Founder" to "Romulus"))),
        Struct(schema, listOf("Britain", mapOf("Prime Minster" to "Churchill", "Monarch" to "Queen Elizabeth I"))),
      )

      val writer = Parquet.writer(Paths.get("/tmp/centurion.parquet"), Configuration(), schema, ParquetFileWriter.Mode.OVERWRITE)
      writer.write(structs)
      writer.close()

      val df = spark.read().parquet("file:////tmp/centurion.parquet")
      df.count() shouldBe 2

      val civilizations = df.select("civilization").collectAsList()
      civilizations[0].get(0) shouldBe "Rome"
      civilizations[1].get(0) shouldBe "Britain"

      val people = df.select("people").collectAsList()
      (people[0].get(0) as scala.collection.Map<*, *>).mkString() shouldBe "Dictator -> CaesarEmperor -> AugustusFounder -> Romulus"
      (people[1].get(0) as scala.collection.Map<*, *>).mkString() shouldBe "Prime Minster -> ChurchillMonarch -> Queen Elizabeth I"
    }
  }
}

