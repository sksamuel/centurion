package com.sksamuel.reactivehive.akkastream

import akka.stream.stage.{GraphStageLogic, GraphStageWithMaterializedValue, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.sksamuel.reactivehive.{PartitionPlan, Struct}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.api.Table

import scala.concurrent.{Future, Promise}

case class HiveSinkSettings()

class HiveSink(db: String, tableName: String, settings: HiveSinkSettings)
              (implicit client: IMetaStoreClient, fs: FileSystem)
  extends GraphStageWithMaterializedValue[SinkShape[Struct], Future[Long]] {

  private val in = Inlet[Struct]("HiveSink.out")

  override def shape: SinkShape[Struct] = SinkShape.of(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Long]) = {

    val promise = Promise[Long]
    val logic = new GraphStageLogic(shape) with InHandler {
      setHandler(in, this)

      //  val partitioner = new CachedPartitionPolicy(config.partitioningPolicy)
      var count = 0L

      var table: Table = _
      var plan: Option[PartitionPlan] = _

      //
      //      private def init(schema: StructType): Unit = {
      //
      //        def getOrCreateTable: Table = {
      //          def create = createTable(db, tableName, schema, settings.partitions, settings.tableType, settings.format)
      //
      //          //     logger.debug(s"Fetching or creating table for schema $schema")
      //          client.tableExists(db.value, tableName.value) match {
      //            case true if settings.overwriteTable =>
      //              dropTable(db, tableName, true)
      //              create
      //            case true => client.getTable(db.value, tableName.value)
      //            case false if settings.createTable => create
      //            case false => throw new RuntimeException(s"Table $db $tableName does not exist")
      //          }
      //        }
      //
      //        if (table == null) {
      //          table = getOrCreateTable
      //          plan = partitionPlan(table)
      //          metastoreSchema = Schemas.scoop(table.getSd.getCols.asScala, table.getPartitionKeys.asScala)
      //          writeSchema = plan.fold(metastoreSchema)(settings.writeSchemaPolicy(metastoreSchema, _)(client))
      //          congruenceMapper = settings.congruenceMapper(schema, writeSchema).getOrElse {
      //            sys.error("Write schema is not compatible with the input schema")
      //          }
      //        }
      //      }
      //
      //      type Writers = Map[Path, HiveWriter]
      //
      //      // takes an existing map of writers and ensures that there is a
      //      // writer open for the given directory
      //      def open(dir: Path, schema: StructType, writers: Writers): Writers = {
      //        writers.get(dir) match {
      //          case None =>
      //            //     logger.debug(s"Creating writer for $dir")
      //            val path = new Path(dir, settings.outputFilePolicy.filename())
      //            //    logger.debug(s"Writer will use output file at $path")
      //            val writer = settings.format.writer(path, schema)
      //            writers + (dir -> writer)
      //          case Some(_) => writers
      //        }
      //      }
      //
      //      /**
      //        * Returns the appropriate output directory for the given struct.
      //        *
      //        * If the table is not partitioned, then the directory returned
      //        * will be the table location, otherwise it will delegate to
      //        * the partitioning policy.
      //        */
      //      def outputDir(struct: Struct, plan: Option[PartitionPlan], table: Table): Try[Path] = {
      //        plan match {
      //          case Some(plan) =>
      //            val part = partition(struct, plan.getKeys)
      //            partitioner(part, db, tableName)(client, fs)
      //          case _ => Try {
      //            new Path(table.getSd.getLocation)
      //          }
      //        }
      //      }
      //
      //      def write(struct: Struct): Unit = {
      //        init(struct.schema)
      //        outputDir(struct, plan, table) match {
      //          case Failure(t) => throw t
      //          case Success(dir) =>
      //            writers = open(dir, writeSchema, writers)
      //            val writer = writers(dir)
      //            val mapped = congruenceMapper(struct)
      //            writer.write(mapped)
      //        }
      //      }
      //
      //      override def onPush(): Unit = {
      //        val struct = grab(in)
      //        init(struct.schema)
      //        try {
      //          write(struct)
      //          count = count + 1
      //          pull(in)
      //        } catch {
      //          case t: Throwable =>
      //            failStage(t)
      //            promise.tryFailure(t)
      //        }
      //      }

      override def preStart(): Unit = pull(in)

      override def postStop(): Unit = {
        promise.trySuccess(count)
      }

      override def onUpstreamFailure(t: Throwable): Unit = {
        promise.tryFailure(t)
      }

      override def onPush(): Unit = ???
    }

    (logic, promise.future)
  }
}
