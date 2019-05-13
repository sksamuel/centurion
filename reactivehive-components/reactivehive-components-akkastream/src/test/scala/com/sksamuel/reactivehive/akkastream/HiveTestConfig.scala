package com.sksamuel.reactivehive.akkastream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

trait HiveTestConfig {

  implicit val hiveConf: HiveConf = new HiveConf()
  hiveConf.set("hive.metastore", "thrift")
  hiveConf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

  implicit val client: HiveMetaStoreClient = new HiveMetaStoreClient(hiveConf)

  implicit val conf: Configuration = new Configuration()
  conf.set("fs.defaultFS", "hdfs://namenode:8020")

  implicit val fs: FileSystem = FileSystem.get(conf)
}
