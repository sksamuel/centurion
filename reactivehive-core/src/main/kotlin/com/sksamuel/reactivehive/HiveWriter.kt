package com.sksamuel.reactivehive

import com.sksamuel.reactivehive.formats.StructWriter
import com.sksamuel.reactivehive.partitioners.Partitioner
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
 * This class will manage multiple writers per partition, but is not thread safe.
 * ! Do not invoke any methods concurrently !
 * It is safe to use multiple instances of this class to write into the same table, as each
 * will use seperate output files.
 *
 * Before a [Struct] is written to Hive, three things must occur.
 *
 * 1. The schema in the metastore must be compatible with the schema in the struct.
 *    This means either evolving the schema or adjusting the incoming struct.
 *
 * 2. If the table has partitions then the struct's partition must exist.
 *
 */
class HiveWriter(private val dbName: DatabaseName,
                 private val tableName: TableName,
    // the write mode determines if the table should be created and/or overwritten, or just appended to
                 private val mode: WriteMode,
                 private val partitioner: Partitioner,
                 private val fileManager: FileManager,
                 private val createConfig: CreateTableConfig?,
                 private val client: IMetaStoreClient,
                 private val fs: FileSystem) {

  // the delegated struct writers, one per partition file
  private val writers = mutableMapOf<Path, StructWriter>()

  private val table = when (mode) {
    WriteMode.Create -> {
      if (createConfig == null)
        throw IllegalArgumentException("CreateTableConfig cannot be null if mode is WriteMode.Create")
      getOrCreateTable(dbName, tableName, createConfig, client, fs)
    }
    WriteMode.Overwrite -> {
      if (createConfig == null)
        throw IllegalArgumentException("CreateTableConfig cannot be null if mode is WriteMode.Overwrite")
      dropTable(dbName, tableName, client, fs)
      getOrCreateTable(dbName, tableName, createConfig, client, fs)
    }
    WriteMode.Write -> client.getTable(dbName.value, tableName.value)
  }

  private val schema = FromHiveSchema.fromHiveTable(table)
  private val plan = partitionPlan(table)
  private val format = serde(table).toFormat()

  // returns a hive writer for the given dir, or creates one if one does not already exist.
  private fun getOrOpen(dir: Path): StructWriter {
    return writers.getOrPut(dir) {
      val path = fileManager.prepare(dir, fs)
      format.writer(path, schema, fs.conf)
    }
  }

  // the directory where the data file will be written to
  // if there are no partitions, then it will be in the table root
  // otherwise, we'll use a partitioner to work out where it's going
  fun outputDir(struct: Struct): Path {
    return if (plan == null) {
      Path(table.sd.location)
    } else {
      val partition = partition(struct, plan)
      partitioner.path(dbName, tableName, partition, client, fs)
    }
  }

  fun write(struct: Struct) {
    val dir = outputDir(struct)
    val writer = getOrOpen(dir)
    writer.write(struct)
  }

  fun write(structs: List<Struct>) = structs.forEach { write(it) }

  fun close() {
    writers.forEach {
      it.value.close()
      fileManager.complete(it.key, fs)
    }
    writers.clear()
  }
}