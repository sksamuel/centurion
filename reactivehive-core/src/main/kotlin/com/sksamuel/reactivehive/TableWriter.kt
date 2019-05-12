package com.sksamuel.reactivehive

import com.sksamuel.reactivehive.formats.Format
import com.sksamuel.reactivehive.formats.HiveWriter
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.TableType

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
class TableWriter(private val dbName: DatabaseName,
                  private val tableName: TableName,
                  private val schema: StructType,
                  private val namer: FileNamer,
    // the write mode determines if the table should be created and/or overwritten, or just appended to
                  private val mode: WriteMode,
                  private val tableType: TableType,
    // the plan to use when creating the table
    // if the table already exists, then the plan from the existing table will be used instead
                  private val _plan: PartitionPlan?,
    // if the table is being created, then the format specified here will be used
                  private val format: Format,
    // when creating new partitions, the partitions locator will be used for the path
                  private val locator: PartitionLocator,
                  private val client: IMetaStoreClient,
                  private val fs: FileSystem) {

  // the delegated hive writers, one per partition path
  private val writers = mutableMapOf<Path, HiveWriter>()

  private val table = when (mode) {
    WriteMode.Create -> getOrCreateTable(dbName, tableName, schema, _plan, tableType, format, client, fs)
    WriteMode.Overwrite -> {
      dropTable(dbName, tableName, client, fs)
      getOrCreateTable(dbName, tableName, schema, _plan, tableType, format, client, fs)
    }
    WriteMode.Write -> client.getTable(dbName.value, tableName.value)
  }

  private val plan = partitionPlan(table)

  // returns a hive writer for the given path, or creates one if one does not already exist.
  private fun getOrOpen(path: Path): HiveWriter {
    return writers.getOrPut(path) {
      format.writer(path, schema, fs.conf)
    }
  }

  fun write(struct: Struct) {
    val path = outputFile(struct, plan, table, locator, namer)
    val writer = getOrOpen(path)
    writer.write(struct)
  }

  fun close() {
    writers.forEach { it.value.close() }
    writers.clear()
  }
}