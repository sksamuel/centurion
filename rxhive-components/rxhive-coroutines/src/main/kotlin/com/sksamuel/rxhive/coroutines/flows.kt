package com.sksamuel.rxhive.coroutines

import com.sksamuel.rxhive.DatabaseName
import com.sksamuel.rxhive.HiveReader
import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.TableName
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.IMetaStoreClient

@UseExperimental(FlowPreview::class)
fun source(db: DatabaseName, table: TableName, client: IMetaStoreClient, fs: FileSystem): Flow<Struct> = flow {
  val reader = HiveReader(db, table, null, client, fs)
  generateSequence { reader.read() }.forEach { emit(it) }
  reader.close()
}