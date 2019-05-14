package com.sksamuel.reactivehive.akkastream

import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.SinkShape
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.InHandler
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.parquet.ToParquetSchema
import com.sksamuel.reactivehive.parquet.parquetWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.Tuple2
import java.util.concurrent.CompletableFuture

class ParquetSink(val path: Path,
                  val conf: Configuration,
                  val schema: StructType) : GraphStageWithMaterializedValue<SinkShape<Struct>, CompletableFuture<Int>>() {

  private val inlet: Inlet<Struct> = Inlet.create("ParquetSink.in")
  override fun shape(): SinkShape<Struct> = SinkShape.of(inlet)

  override fun createLogicAndMaterializedValue(inheritedAttributes: Attributes?): Tuple2<GraphStageLogic, CompletableFuture<Int>> {

    val future = CompletableFuture<Int>()

    val graph: GraphStageLogic = object : GraphStageLogic(shape()), InHandler {

      private val writer = parquetWriter(path, conf, ToParquetSchema.toMessageType(schema))
      private var count = 0

      init {
        setHandler(inlet, this)
      }

      override fun beforePreStart() {
        pull(inlet)
      }

      override fun onPush() {
        val struct = grab(inlet)
        try {
          writer.write(struct)
          count++
          pull(inlet)
        } catch (t: Throwable) {
          failStage(t)
          future.completeExceptionally(t)
          writer.close()
        }
      }

      override fun onUpstreamFinish() {
        future.complete(count)
        writer.close()
        completeStage()
      }

      override fun onUpstreamFailure(t: Throwable?) {
        future.completeExceptionally(t)
        writer.close()
        failStage(t)
      }
    }

    return Tuple2(graph, future)
  }
}