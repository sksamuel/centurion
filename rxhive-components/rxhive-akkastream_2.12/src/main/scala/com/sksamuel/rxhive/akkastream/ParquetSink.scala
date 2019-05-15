package com.sksamuel.rxhive.akkastream

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.rxhive.parquet.{ParquetWriterKt, ParquetWriterSettings, ToParquetSchema}
import com.sksamuel.rxhive.{Struct, StructType}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.concurrent.{Future, Promise}

class ParquetSink(val path: Path,
                  val conf: Configuration,
                  val schema: StructType,
                  val overwrite: Boolean = false) extends GraphStageWithMaterializedValue[SinkShape[Struct], Future[Long]]() {

  private val inlet: Inlet[Struct] = Inlet.create("ParquetSink.in")
  override def shape(): SinkShape[Struct] = SinkShape.of(inlet)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val promise = Promise[Long]

    val graph: GraphStageLogic = new GraphStageLogic(shape()) with InHandler {

      private val writer = ParquetWriterKt.parquetWriter(path, conf, ToParquetSchema.INSTANCE.toMessageType(schema, "struct"), overwrite, new java.util.HashMap(), new ParquetWriterSettings())
      private var count = 0L

      setHandler(inlet, this)

      override def beforePreStart() {
        pull(inlet)
      }

      override def onPush() {
        val struct = grab(inlet)
        try {
          writer.write(struct)
          count = count + 1
          pull(inlet)
        } catch {
          case t: Throwable =>
            failStage(t)
            writer.close()
            promise.tryFailure(t)
        }
      }

      override def onUpstreamFinish() {
        writer.close()
        completeStage()
        promise.trySuccess(count)
      }

      override def onUpstreamFailure(t: Throwable) {
        writer.close()
        failStage(t)
        promise.tryFailure(t)
      }
    }

    (graph, promise.future)
  }
}