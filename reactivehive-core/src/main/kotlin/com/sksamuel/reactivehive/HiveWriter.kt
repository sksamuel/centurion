package com.sksamuel.reactivehive

import com.sksamuel.reactivehive.formats.StructWriter
import com.sksamuel.reactivehive.schemas.FromHiveSchema
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient

enum class WriteMode {
  Create, Overwrite, Write
}

/**
 * Responsible for writing data to a hive table.
 * The writer will handle partitioning if required, formats, creating the table if needed.
 * This class will manage multiple writers per partition, but is not thread safe. Do not
 * invoke any methods concurrently.
 * It is safe to use multiple instances of this class to write into the same table, as each
 * will use seperate output files.
 */
class HiveWriter(private val dbName: DatabaseName,
                 private val tableName: TableName,
                 private val namer: FileNamer,
    // the write mode determines if the table should be created and/or overwritten, or just appended to
                 private val mode: WriteMode,
    // when creating new partitions, the partitions locator will be used for the path
                 private val locator: PartitionLocator,
                 private val createConfig: CreateTableConfig,
                 private val client: IMetaStoreClient,
                 private val fs: FileSystem) {

  // the delegated hive writers, one per partition path
  private val writers = mutableMapOf<Path, StructWriter>()

  private val table = when (mode) {
    WriteMode.Create -> getOrCreateTable(dbName, tableName, createConfig, client, fs)
    WriteMode.Overwrite -> {
      dropTable(dbName, tableName, client, fs)
      getOrCreateTable(dbName, tableName, createConfig, client, fs)
    }
    WriteMode.Write -> client.getTable(dbName.value, tableName.value)
  }

  private val schema = FromHiveSchema.fromHiveTable(table)
  private val plan = partitionPlan(table)
  private val format = serde(table).toFormat()

  // returns a hive writer for the given path, or creates one if one does not already exist.
  private fun getOrOpen(path: Path): StructWriter {
    return writers.getOrPut(path) {
      format.writer(path, schema, fs.conf)
    }
  }

  fun write(struct: Struct) {
    val path = outputFile(struct, plan, table, locator, namer)
    val writer = getOrOpen(path)
    writer.write(struct)
  }

  fun write(structs: List<Struct>) = structs.forEach { write(it) }

  fun close() {
    writers.forEach { it.value.close() }
    writers.clear()
  }
}