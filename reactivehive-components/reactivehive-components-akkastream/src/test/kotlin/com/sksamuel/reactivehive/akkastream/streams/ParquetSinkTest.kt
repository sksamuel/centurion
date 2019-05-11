package com.sksamuel.reactivehive.akkastream.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.Materializer
import akka.stream.javadsl.Source
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import io.kotlintest.matchers.string.shouldNotBeEmpty
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.concurrent.TimeUnit

class ParquetSinkTest : FunSpec() {

  private val conf = Configuration()
  private val fs = FileSystem.getLocal(conf)

  private val system = ActorSystem.create("test")
  private val materializer: Materializer = ActorMaterializer.create(system)

  init {
    test("writing structs with parquet sink") {

      val schema = StructType(StructField("a", StringType), StructField("b", StringType))
      val struct = Struct(schema, "hello", "world")

      val path = Path("parquet.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      val sink = ParquetSink(path, conf, schema)
      val f = Source.from(listOf(struct, struct, struct, struct)).runWith(sink, materializer)

      val result = f.get(1, TimeUnit.MINUTES)
      result shouldBe 4

      Thread.sleep(2000)

      fs.open(path).bufferedReader().readText().shouldNotBeEmpty()
    }
  }
}