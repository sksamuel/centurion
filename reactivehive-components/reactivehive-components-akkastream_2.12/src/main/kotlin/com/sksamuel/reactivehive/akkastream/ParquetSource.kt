package com.sksamuel.reactivehive.akkastream

import akka.stream.Attributes
import akka.stream.Outlet
import akka.stream.SourceShape
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.OutHandler
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.parquet.parquetReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.Tuple2
import java.util.concurrent.CompletableFuture

class ParquetSource(val path: Path,
                    val conf: Configuration) : GraphStageWithMaterializedValue<SourceShape<Struct>, CompletableFuture<Int>>() {

  private val outlet: Outlet<Struct> = Outlet.create("ParquetSource.out")
  override fun shape(): SourceShape<Struct> = SourceShape.of(outlet)

  override fun createLogicAndMaterializedValue(inheritedAttributes: Attributes?): Tuple2<GraphStageLogic, CompletableFuture<Int>> {

    val future = CompletableFuture<Int>()

    val graph: GraphStageLogic = object : GraphStageLogic(shape()), OutHandler {

      private val reader = parquetReader(path, conf)
      private var count = 0

      init {
        setHandler(outlet, this)
      }

      override fun onPull() {
        val struct = reader.read()
        try {
          if (struct == null) {
            completeStage()
          } else {
            push(outlet, struct)
            count++
          }
        } catch (t: Throwable) {
          failStage(t)
        }
      }
    }

    return Tuple2(graph, future)
  }
}