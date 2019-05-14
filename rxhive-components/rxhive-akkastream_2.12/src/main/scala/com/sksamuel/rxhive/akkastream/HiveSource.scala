package com.sksamuel.rxhive.akkastream

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.sksamuel.rxhive.{DatabaseName, HiveReader, PartitionPushdown, Struct, TableName}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.concurrent.{Future, Promise}

case class HiveSourceSettings(pushdown: Option[PartitionPushdown] = None)

class HiveSource(dbName: String, tableName: String, settings: HiveSourceSettings = HiveSourceSettings())
                (implicit client: IMetaStoreClient, fs: FileSystem) extends GraphStageWithMaterializedValue[SourceShape[Struct], Future[Long]] {

  private val out = Outlet[Struct]("HiveSource.out")
  override def shape: SourceShape[Struct] = SourceShape.of(out)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val promise = Promise[Long]

    val logic: GraphStageLogic = new GraphStageLogic(shape) with OutHandler {
      setHandler(out, this)

      private var count = 0
      private lazy val reader = new HiveReader(
        new DatabaseName(dbName),
        new TableName(tableName),
        settings.pushdown.orNull,
        client,
        fs
      )

      override def onPull(): Unit = {
        try {
          val struct = reader.read()
          if (struct == null) {
            completeStage()
            reader.close()
            promise.trySuccess(count)
          } else {
            push(out, struct)
            count = count + 1
          }
        } catch {
          case t: Throwable =>
            failStage(t)
            reader.close()
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
