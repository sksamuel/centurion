package com.sksamuel.rxhive.akkastream

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.rxhive.evolution.{NoopSchemaEvolver, SchemaEvolver}
import com.sksamuel.rxhive.formats.{Format, ParquetFormat}
import com.sksamuel.rxhive.partitioners.{DynamicPartitioner, Partitioner}
import com.sksamuel.rxhive.resolver.{LenientStructResolver, StructResolver}
import com.sksamuel.rxhive.{CreateTableConfig, DatabaseName, FileManager, HiveWriter, RxHiveFileNamer, StagingFileManager, Struct, TableName, WriteMode}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, TableType}

import scala.concurrent.{Future, Promise}

case class HiveSinkSettings(mode: WriteMode = WriteMode.Create,
                            partitioner: Partitioner = DynamicPartitioner.INSTANCE,
                            resolver: StructResolver = LenientStructResolver.INSTANCE,
                            evolver: SchemaEvolver = NoopSchemaEvolver.INSTANCE,
                            fileManager: FileManager = new StagingFileManager(RxHiveFileNamer.INSTANCE),
                            tableType: TableType = TableType.MANAGED_TABLE,
                            externalLocation: Option[Path] = None,
                            format: Format = ParquetFormat.INSTANCE)

class HiveSink(dbName: String, tableName: String, settings: HiveSinkSettings)
              (implicit client: IMetaStoreClient, fs: FileSystem)
  extends GraphStageWithMaterializedValue[SinkShape[Struct], Future[Long]] {

  private val in = Inlet[Struct]("HiveSink.in")

  override def shape: SinkShape[Struct] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val promise = Promise[Long]
    val logic: GraphStageLogic with InHandler = new GraphStageLogic(shape) with InHandler {
      setHandler(in, this)

      private var count = 0L
      private var writer: HiveWriter = _

      override def preStart(): Unit = pull(in)

      override def onUpstreamFailure(t: Throwable): Unit = {
        super.onUpstreamFailure(t)
        if (writer != null)
          writer.close()
        failStage(t)
        promise.tryFailure(t)
      }

      override def onUpstreamFinish(): Unit = {
        if (writer != null)
        writer.close()
        completeStage()
        promise.trySuccess(count)
      }

      override def onPush(): Unit = {
        try {
          val struct = grab(in)
          if (writer == null)
            writer = new HiveWriter(
              new DatabaseName(dbName),
              new TableName(tableName),
              settings.mode,
              settings.partitioner,
              settings.fileManager,
              settings.evolver,
              settings.resolver,
              new CreateTableConfig(struct.getSchema, null, settings.tableType, settings.format, settings.externalLocation.orNull),
              client,
              fs
            )
          writer.write(struct)
          count = count + 1
          pull(in)
        } catch {
          case t: Throwable =>
            promise.tryFailure(t)
            if (writer != null)
              writer.close()
            failStage(t)
        }
      }
    }

    (logic, promise.future)
  }
}
