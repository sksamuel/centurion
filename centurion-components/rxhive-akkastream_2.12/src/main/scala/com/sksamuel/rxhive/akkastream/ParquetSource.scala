package com.sksamuel.rxhive.akkastream

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.sksamuel.rxhive.Struct
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.concurrent.{Future, Promise}

class ParquetSource(val path: Path,
                    val conf: Configuration) extends GraphStageWithMaterializedValue[SourceShape[Struct], Future[Long]]() {

  private val outlet: Outlet[Struct] = Outlet.create("ParquetSource.out")
  override def shape(): SourceShape[Struct] = SourceShape.of(outlet)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val promise = Promise[Long]

    val graph: GraphStageLogic = new GraphStageLogic(shape()) with OutHandler {

      private val reader = com.sksamuel.rxhive.parquet.ParquetReaderKt.parquetReader(path, conf)
      private var count = 0L

      setHandler(outlet, this)

      override def onPull() {
        val struct = reader.read()
        try {
          if (struct == null) {
            completeStage()
            promise.trySuccess(count)
          } else {
            push(outlet, struct)
            count = count + 1
          }
        } catch {
          case t: Throwable =>
            failStage(t)
            promise.tryFailure(t)
        }
      }
    }

    Tuple2(graph, promise.future)
  }
}