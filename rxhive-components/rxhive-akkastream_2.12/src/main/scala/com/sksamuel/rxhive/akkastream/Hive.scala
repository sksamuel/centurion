package com.sksamuel.rxhive.akkastream

import akka.stream.scaladsl.{Sink, Source}
import com.sksamuel.rxhive.Struct
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scala.concurrent.Future

object Hive {

  def source(db: String, table: String)
            (implicit client: IMetaStoreClient, fs: FileSystem): Source[Struct, Future[Long]] =
    Source.fromGraph(new HiveSource(db, table))

  def sink(db: String, table: String, settings: HiveSinkSettings)
          (implicit client: IMetaStoreClient, fs: FileSystem): Sink[Struct, Future[Long]] =
    Sink.fromGraph(new HiveSink(db, table, settings))
}
