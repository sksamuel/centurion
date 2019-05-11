package com.sksamuel.reactivehive.akkastream.streams

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.Materializer
import akka.stream.javadsl.Sink
import akka.stream.javadsl.Source
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TimestampMillisType
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

class ParquetRoundTripTest : FunSpec() {

  private val conf = Configuration()
  private val fs = FileSystem.get(conf)

  private val system = ActorSystem.create("test")
  private val materializer: Materializer = ActorMaterializer.create(system)

  init {
    test("read and write") {

      val schema = StructType(
          StructField("a", StringType),
          StructField("b", TimestampMillisType),
          StructField("c", BooleanType)
      )

      val expected = Struct(schema, "hello world", Timestamp.valueOf(LocalDateTime.of(1979, 9, 10, 2, 3, 4)), true)

      val path = Path("parquetrt.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      val sink = ParquetSink(path, conf, schema)
      val f1 = Source.from(List(10) { expected }).runWith(sink, materializer)
      f1.get(1, TimeUnit.MINUTES) shouldBe 10

      Thread.sleep(1000)

      val f2 = Source
          .fromGraph(ParquetSource(Path("parquetrt.pq"), conf))
          .runWith(Sink.seq(), materializer)

      val actual = f2.toCompletableFuture().get(1, TimeUnit.MINUTES).first()

      actual shouldBe expected
    }
  }
}