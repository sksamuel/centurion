package com.sksamuel.reactivehive.akkastream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.Materializer
import akka.stream.javadsl.Sink
import akka.stream.javadsl.Source
import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.DecimalType
import com.sksamuel.reactivehive.Float32Type
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.Int16Type
import com.sksamuel.reactivehive.Int32Type
import com.sksamuel.reactivehive.Int64Type
import com.sksamuel.reactivehive.Precision
import com.sksamuel.reactivehive.Scale
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TimestampMillisType
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.math.BigDecimal
import java.math.BigInteger
import java.math.MathContext
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.util.concurrent.TimeUnit

class ParquetSourceTest : FunSpec() {

  private val conf = Configuration()
  private val fs = FileSystem.getLocal(conf)

  private val system = ActorSystem.create("test")
  private val materializer: Materializer = ActorMaterializer.create(system)

  init {
    test("reading with parquet source") {

      val f = Source
          .fromGraph(ParquetSource(Path(this.javaClass.getResource("/spark.parquet").toURI()),
              conf))
          .runWith(Sink.seq(), materializer)

      val struct = f.toCompletableFuture().get(1, TimeUnit.MINUTES).first()
      struct.schema shouldBe StructType(
          fields = listOf(
              StructField(name = "myDouble", type = Float64Type, nullable = true),
              StructField(name = "myLong", type = Int64Type, nullable = true),
              StructField(name = "myInt", type = Int32Type, nullable = true),
              StructField(name = "myBoolean", type = BooleanType, nullable = true),
              StructField(name = "myFloat", type = Float32Type, nullable = true),
              StructField(name = "myShort", type = Int16Type, nullable = true),
              StructField(name = "myDecimal", type = DecimalType(Precision(38), Scale(18)), nullable = true),
              StructField(name = "myBytes", type = BinaryType, nullable = true),
              StructField(name = "myDate", type = DateType, nullable = true),
              StructField(name = "myTimestamp", type = TimestampMillisType, nullable = true)
          )
      )

      struct.values[0] shouldBe 13.46
      struct.values[1] shouldBe 1414
      struct.values[2] shouldBe 239
      struct.values[3] shouldBe true
      struct.values[4] shouldBe 1825.5
      struct.values[5] shouldBe 12
      struct.values[6] shouldBe
          BigDecimal(BigInteger.valueOf(727200000000000000L).multiply(BigInteger.valueOf(100)), 18, MathContext(38))
      struct.values[7] shouldNotBe null
      struct.values[8] shouldBe LocalDate.of(3879, 10, 10)
      struct.values[9] shouldBe Timestamp.from(Instant.ofEpochMilli(11155523123L))
    }
  }
}