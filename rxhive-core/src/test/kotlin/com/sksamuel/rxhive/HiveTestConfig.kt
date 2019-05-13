package com.sksamuel.rxhive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.MetaException

object HiveTestConfig {

  val hiveConf: HiveConf by lazy {
    HiveConf().apply {
      set("hive.metastore", "thrift")
      set("hive.metastore.uris", "thrift://hive-metastore:9083")
    }
  }

  val client: HiveMetaStoreClient by lazy {
    try {
      HiveMetaStoreClient(hiveConf)
    } catch (t: MetaException) {
      println("Error $t")
      t.printStackTrace()
      throw t
    }
  }

  val conf: Configuration by lazy {
    Configuration().apply {
      set("fs.defaultFS", "hdfs://namenode:8020")
    }
  }

  val fs: FileSystem by lazy {
    FileSystem.get(conf)
  }
}