package com.sksamuel.rxhive.akkastream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.rxhive.{BooleanType, Float64Type, StringType, Struct, StructField, StructType}
import org.apache.hadoop.hive.metastore.api.{Database, NoSuchObjectException}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class HiveSourceTest extends FunSuite with Matchers with HiveTestConfig {

  private implicit val system: ActorSystem = ActorSystem()
  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  val schema = new StructType(
    new StructField("name", StringType.INSTANCE),
    new StructField("title", StringType.INSTANCE),
    new StructField("salary", Float64Type.INSTANCE),
    new StructField("employed", BooleanType.INSTANCE)
  )

  val users = List(
    new Struct(schema, "sam", "mr", java.lang.Double.valueOf(100.43), java.lang.Boolean.valueOf(false)),
    new Struct(schema, "ben", "mr", java.lang.Double.valueOf(230.523), java.lang.Boolean.valueOf(false)),
    new Struct(schema, "tom", "mr", java.lang.Double.valueOf(60.98), java.lang.Boolean.valueOf(true)),
    new Struct(schema, "laura", "ms", java.lang.Double.valueOf(421.512), java.lang.Boolean.valueOf(true)),
    new Struct(schema, "kelly", "ms", java.lang.Double.valueOf(925.162), java.lang.Boolean.valueOf(false))
  )

  Try {
    client.createDatabase(new Database("tests", null, "/user/hive/warehouse/tests", new java.util.HashMap()))
  }

  test("hive sink happy path") {

    Try {
      client.dropTable("tests", "hivesrc")
    }

    val f = Source(users)
      .runWith(new HiveSink("tests", "hivesrc", HiveSinkSettings()))

    Await.result(f, 10.seconds) shouldBe 5

    val g = Hive.source("tests", "hivesrc").runWith(Sink.seq)
    val results = Await.result(g, 10.seconds)

    results shouldBe Vector(
      new Struct(schema, "sam", "mr", java.lang.Double.valueOf(100.43), java.lang.Boolean.valueOf(false)),
      new Struct(schema, "ben", "mr", java.lang.Double.valueOf(230.523), java.lang.Boolean.valueOf(false)),
      new Struct(schema, "tom", "mr", java.lang.Double.valueOf(60.98), java.lang.Boolean.valueOf(true)),
      new Struct(schema, "laura", "ms", java.lang.Double.valueOf(421.512), java.lang.Boolean.valueOf(true)),
      new Struct(schema, "kelly", "ms", java.lang.Double.valueOf(925.162), java.lang.Boolean.valueOf(false))
    )

  }

  test("invalid database should fail future") {
    val f = Source(users)
      .runWith(new HiveSink("qeqweqweew", "users", HiveSinkSettings()))
    intercept[NoSuchObjectException] {
      Await.result(f, 10.seconds) shouldBe 5
    }
  }
}
