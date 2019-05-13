package com.sksamuel.reactivehive

import com.sksamuel.reactivehive.evolution.NoopSchemaEvolver
import com.sksamuel.reactivehive.evolution.SchemaEvolver
import com.sksamuel.reactivehive.formats.StructWriter
import com.sksamuel.reactivehive.partitioners.DynamicPartitioner
import com.sksamuel.reactivehive.partitioners.Partitioner
import com.sksamuel.reactivehive.resolver.LenientStructResolver
import com.sksamuel.reactivehive.resolver.StructResolver
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
                 private val mode: WriteMode = WriteMode.Write,
                 private val partitioner: Partitioner = DynamicPartitioner,
                 private val fileManager: FileManager = StagingFileManager(),
                 private val evolver: SchemaEvolver = NoopSchemaEvolver,
                 private val resolver: StructResolver = LenientStructResolver,
                 private val createConfig: CreateTableConfig? = null,
                 private val client: IMetaStoreClient,
                 private val fs: FileSystem) {

  // the delegated struct writers, one per partition file
  private val writers = mutableMapOf<Path, StructWriter>()

  private var table = when (mode) {
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
    WriteMode.Write -> loadTable()
  }

  private var metastoreSchema = FromHiveSchema.fromHiveTable(table)
  private val plan = partitionPlan(table)
  private val format = serde(table).toFormat()

  private fun loadTable() = client.getTable(dbName.value, tableName.value)

  // returns a hive writer for the given struct, creating one if one does not already exist.
  // creating a new writer uses a file manager to handle the target file
  private fun getOrOpenWriter(partition: Partition?, writeSchema: StructType): StructWriter {
    val dir = outputDir(partition)
    return writers.getOrPut(dir) {
      val path = fileManager.prepare(dir, fs)
      format.writer(path, writeSchema, fs.conf)
    }
  }

  // the directory where the data file will be written to
  // if there are no partitions, then it will be in the table root
  // otherwise, we'll use a partitioner to work out where it's going
  fun outputDir(partition: Partition?): Path {
    return if (partition == null) {
      Path(table.sd.location)
    } else {
      partitioner.path(dbName, tableName, partition, client, fs)
    }
  }

  fun write(struct: Struct) {

    // the first thing we must do is ensure the metastore schema is up to date
    // and that the struct is aligned against the metastore
    metastoreSchema = evolver.evolve(dbName, tableName, metastoreSchema, struct, client)
    val resolvedStruct = resolver.resolve(struct, metastoreSchema)

    // calculate the partition to be used if any
    val partition = if (plan == null) null else partition(resolvedStruct, plan)

    // calculate the write schema
    val writeSchema = if (plan == null) metastoreSchema else DefaultDiskSchemas.writerSchema(resolvedStruct, metastoreSchema, plan)

    // grab a writer for the partition
    val writer = getOrOpenWriter(partition, writeSchema)

    // align the struct to be written with the write schema
    val finalStruct = align(resolvedStruct, writeSchema)

    writer.write(finalStruct)
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