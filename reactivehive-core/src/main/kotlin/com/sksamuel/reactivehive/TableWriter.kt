package com.sksamuel.reactivehive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.metastore.api.Table
import java.util.*

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
                  private val format: Format,
                  private val schema: StructType,
                  private val client: IMetaStoreClient,
                  private val fs: FileSystem) {

  // the delegated hive writers, one per partition path
  private val writers = mutableMapOf<Path, HiveWriter>()

  private val locator = DefaultPartitionLocator

  private val table = createTable(
      dbName,
      tableName,
      schema,
      PartitionPlan.empty,
      TableType.MANAGED_TABLE,
      client = client,
      fs = fs
  )

  private val tableBasePath = Path(table.sd.location)

  private val plan = partitionPlan(table)

  // returns a hive writer for the given path, or creates one if one does not already exist.
  private fun getOrOpen(path: Path): HiveWriter {
    return writers.getOrPut(path) {
      format.writer(path, schema, fs.conf)
    }
  }

  /**
   * Returns the path to the file that this struct should be written to.
   * If the table is partitioned then this will be the partition path, otherwise
   * the base table location.
   */
  private fun outputFile(struct: Struct, plan: PartitionPlan?, table: Table): Path {
    return when (plan) {
      null -> Path(table.sd.location)
      else -> {
        val partition = partition(struct, plan)
        val dir = locator.path(tableBasePath, partition)
        Path(dir, UUID.randomUUID().toString())
      }
    }
  }

  fun write(struct: Struct) {
    val path = outputFile(struct, plan, table)
    val writer = getOrOpen(path)
    writer.write(struct)
  }

  fun close() {
    writers.forEach { it.value.close() }
    writers.clear()
  }
}