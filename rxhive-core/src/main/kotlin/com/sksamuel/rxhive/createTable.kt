package com.sksamuel.rxhive

import com.sksamuel.rxhive.formats.Format
import com.sksamuel.rxhive.schemas.ToHiveSchema
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

// config for when creating a new table
data class CreateTableConfig(
    val schema: StructType,
    // partitions, can be null if not partitioned
    val plan: PartitionPlan?,
    val tableType: TableType,
    val format: Format,
    // specify a location to be used if the table is created as an external table
    val location: Path?
)

fun createTable(dbName: DatabaseName,
                tableName: TableName,
                createConfig: CreateTableConfig,
                client: IMetaStoreClient,
                fs: FileSystem): Table {

  val params = mutableMapOf<String, String>()
  params["CREATED_BY"] = "rxhive"

  val partitionFieldSchemas = createConfig.plan?.keys?.map {
    FieldSchema(it.value, ToHiveSchema.toHiveType(StringType), null)
  } ?: emptyList()

  if (createConfig.tableType == TableType.EXTERNAL_TABLE)
    params["EXTERNAL"] = "TRUE"

  val sd = StorageDescriptor().apply {
    // must be set correctly for the hive format used to decode when using spark/impala etc
    inputFormat = createConfig.format.serde().inputFormat
    outputFormat = createConfig.format.serde().outputFormat
    serdeInfo = SerDeInfo(
        null,
        createConfig.format.serde().serializationLib,
        createConfig.format.serde().params
    )
    location = when (createConfig.tableType) {
      TableType.EXTERNAL_TABLE -> createConfig.location?.toString()
          ?: throw UnsupportedOperationException("Table location must be specified when creating EXTERNAL TABLE")
      else -> client.getDatabase(dbName.value).locationUri + "/" + tableName.value
    }
    // partition fields must not be included in the list of general columns
    this.cols = ToHiveSchema.toHiveSchema(createConfig.schema).filterNot {
      createConfig.plan?.keys?.contains(PartitionKey(it.name)) ?: false
    }
  }

  val table = Table()
  table.dbName = dbName.value
  table.tableName = tableName.value
  // not sure what this does
  table.owner = "hive"
  table.createTime = (System.currentTimeMillis() / 1000).toInt()
  table.parameters = params
  // the general columns must not include partition fields
  table.partitionKeys = partitionFieldSchemas
  table.tableType = createConfig.tableType.asString()
  table.sd = sd

  client.createTable(table)

  fs.mkdirs(Path(table.sd.location))

  return table
}

fun getOrCreateTable(dbName: DatabaseName,
                     tableName: TableName,
                     createTableConfig: CreateTableConfig,
                     client: IMetaStoreClient,
                     fs: FileSystem): Table {
  return if (client.tableExists(dbName.value, tableName.value)) {
    client.getTable(dbName.value, tableName.value)
  } else createTable(dbName, tableName, createTableConfig, client, fs)
}