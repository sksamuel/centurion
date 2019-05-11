package com.sksamuel.reactivehive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient

interface HiveTestConfig {

  val hiveConf: HiveConf
    get() = HiveConf().apply {
      set("hive.metastore", "thrift")
      set("hive.metastore.uris", "thrift://localhost:9083")
    }

  val client: HiveMetaStoreClient
    get() = HiveMetaStoreClient(hiveConf)

  val conf: Configuration
    get() = Configuration().apply {
      set("fs.defaultFS", "hdfs://localhost:8020")
    }

  val fs: FileSystem
    get() = FileSystem.get(conf)
}