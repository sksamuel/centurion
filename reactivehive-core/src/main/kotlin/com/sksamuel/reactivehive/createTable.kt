package com.sksamuel.reactivehive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.Table

fun TableType.asString(): String {
  return when (this) {
    TableType.MANAGED_TABLE -> "MANAGED_TABLE"
    TableType.EXTERNAL_TABLE -> "EXTERNAL_TABLE"
    TableType.VIRTUAL_VIEW -> "VIEW"
    TableType.INDEX_TABLE -> "INDEX_TABLE"
  }
}

fun createTable(db: DatabaseName,
                tableName: TableName,
                schema: StructType,
                plan: PartitionPlan,
                tableType: TableType,
                format: Format = ParquetFormat,
                client: IMetaStoreClient,
                fs: FileSystem): Table {

  val params = mutableMapOf<String, String>()
  params["CREATED_BY"] = "reactive-hive"

  val partitionFieldSchemas = plan.keys.map {
    FieldSchema(it.value, ToHiveSchema.toHiveType(StringType), null)
  }

  if (tableType == TableType.EXTERNAL_TABLE)
    params["EXTERNAL"] = "TRUE"

  val sd = StorageDescriptor().apply {
    // must be set correctly for the hive format used to decode when using spark/impala etc
    inputFormat = format.serde().inputFormat
    outputFormat = format.serde().outputFormat
    serdeInfo = SerDeInfo(null, format.serde().serializationLib, format.serde().params)
    location = client.getDatabase(db.value).locationUri + "/" + tableName.value
    // partition fields must not be included in the list of general columns
    this.cols = ToHiveSchema.toHiveSchema(schema).filterNot { plan.keys.contains(PartitionKey(it.name)) }
  }

  val table = Table()
  table.dbName = db.value
  table.tableName = tableName.value
  // not sure what this does
  table.owner = "hive"
  table.createTime = (System.currentTimeMillis() / 1000).toInt()
  table.parameters = params
  // the general columns must not include partition fields
  table.partitionKeys = partitionFieldSchemas
  table.tableType = tableType.asString()
  table.sd = sd

  client.createTable(table)

  fs.mkdirs(Path(table.sd.location))

  return table
}