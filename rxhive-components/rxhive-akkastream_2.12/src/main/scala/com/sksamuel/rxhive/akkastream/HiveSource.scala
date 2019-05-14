package com.sksamuel.rxhive.akkastream

import akka.stream.scaladsl.{Sink, Source}
import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.sksamuel.rxhive.{DatabaseName, HiveReader, Struct, TableName}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.concurrent.{Future, Promise}

object Hive {

  def source(db: String, table: String)
            (implicit client: IMetaStoreClient, fs: FileSystem): Source[Struct, Future[Long]] =
    Source.fromGraph(new HiveSource(db, table))

  def sink(db: String, table: String, settings: HiveSinkSettings)
          (implicit client: IMetaStoreClient, fs: FileSystem): Sink[Struct, Future[Long]] =
    Sink.fromGraph(new HiveSink(db, table, settings))
}

class HiveSource(dbName: String, tableName: String)
                (implicit client: IMetaStoreClient, fs: FileSystem) extends GraphStageWithMaterializedValue[SourceShape[Struct], Future[Long]] {

  private val out = Outlet[Struct]("HiveSource.out")

  override def shape: SourceShape[Struct] = SourceShape.of(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val promise = Promise[Long]

    val logic = new GraphStageLogic(shape) with OutHandler {
      setHandler(out, this)

      private var count = 0
      private val reader = new HiveReader(new DatabaseName(dbName), new TableName(tableName), client, fs)

      override def onPull(): Unit = {
        try {
          val struct = reader.read()
          if (struct == null) {
            completeStage()
            promise.trySuccess(count)
          } else {
            push(out, struct)
            count = count + 1
          }
        } catch {
          case t: Throwable =>
            failStage(t)
            promise.tryFailure(t)
        }
      }

      override def onDownstreamFinish(): Unit = {
        completeStage()
        reader.close()
        promise.trySuccess(count)
      }
    }

    (logic, promise.future)
  }
}
