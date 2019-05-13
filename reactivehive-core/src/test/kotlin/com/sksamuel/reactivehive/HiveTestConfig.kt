package com.sksamuel.reactivehive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

object HiveTestConfig {

  val hiveConf: HiveConf by lazy {
    HiveConf().apply {
      set("hive.metastore", "thrift")
      set("hive.metastore.uris", "thrift://localhost:9083")
    }
  }

  val client: HiveMetaStoreClient by lazy {
    HiveMetaStoreClient(hiveConf)
  }

  val conf: Configuration by lazy {
    Configuration().apply {
      set("fs.defaultFS", "hdfs://localhost:8020")
    }
  }

  val fs: FileSystem by lazy {
    FileSystem.get(conf)
  }
}