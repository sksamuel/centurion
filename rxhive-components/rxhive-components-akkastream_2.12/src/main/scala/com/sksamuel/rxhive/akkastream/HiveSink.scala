package com.sksamuel.rxhive.akkastream

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.rxhive.evolution.NoopSchemaEvolver
import com.sksamuel.rxhive.formats.ParquetFormat
import com.sksamuel.rxhive.partitioners.DynamicPartitioner
import com.sksamuel.rxhive.resolver.LenientStructResolver
import com.sksamuel.rxhive.{CreateTableConfig, DatabaseName, HiveWriter, PartitionPlan, RxHiveFileNamer, StagingFileManager, Struct, TableName, WriteMode}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType}

import scala.concurrent.{Future, Promise}

case class HiveSinkSettings()

class HiveSink(db: String, tableName: String, settings: HiveSinkSettings)
              (implicit client: IMetaStoreClient, fs: FileSystem)
  extends GraphStageWithMaterializedValue[SinkShape[Struct], Future[Long]] {

  private val in = Inlet[Struct]("HiveSink.out")

  override def shape: SinkShape[Struct] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val promise = Promise[Long]
    val logic: GraphStageLogic with InHandler = new GraphStageLogic(shape) with InHandler {
      setHandler(in, this)

      var count = 0L

      var table: Table = _
      var plan: Option[PartitionPlan] = _
      private var writer: HiveWriter = _

      override def preStart(): Unit = pull(in)

      override def onUpstreamFailure(t: Throwable): Unit = {
        super.onUpstreamFailure(t)
        promise.tryFailure(t)
      }

      override def onUpstreamFinish(): Unit = {
        promise.trySuccess(count)
      }

      override def onPush(): Unit = {
        try {
          val struct = grab(in)
          if (writer == null)
            writer = new HiveWriter(
              new DatabaseName(db),
              new TableName(tableName),
              WriteMode.Create,
              DynamicPartitioner.INSTANCE,
              new StagingFileManager(RxHiveFileNamer.INSTANCE),
              NoopSchemaEvolver.INSTANCE,
              LenientStructResolver.INSTANCE,
              new CreateTableConfig(struct.getSchema, null, TableType.MANAGED_TABLE, ParquetFormat.INSTANCE, null),
              client,
              fs
            )
          writer.write(struct)
          count = count + 1
          pull(in)
        } catch {
          case t: Throwable =>
            promise.tryFailure(t)
            failStage(t)
        }
      }
    }

    (logic, promise.future)
  }
}
