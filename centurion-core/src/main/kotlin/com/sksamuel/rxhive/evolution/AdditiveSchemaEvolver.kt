package com.sksamuel.rxhive.evolution

import com.sksamuel.rxhive.DatabaseName
import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TableName
import com.sksamuel.rxhive.schemas.FromHiveSchema
import com.sksamuel.rxhive.schemas.ToHiveSchema
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementation of [SchemaEvolver] that will update the metastore by
 * attempting to add new fields in a backwards compatible way.
 *
 * This can be accomplished if the new field is nullable (so that existing data
 * can return null for this field when queried).
 *
 * If any fields are missing from the metastore schema, but are not nullable,
 * then an exception will be thrown.
 */
object AdditiveSchemaEvolver : SchemaEvolver {

  override fun evolve(dbName: DatabaseName,
                      tableName: TableName,
                      metastoreSchema: StructType,
                      struct: Struct,
                      client: IMetaStoreClient): StructType {

    // find any fields that are not present in the metastore and attempt to
    // evolve the metastore to include them
    val toBeAdded = struct.schema.fields.filterNot { metastoreSchema.hasField(it.name) }
    val nonnull = toBeAdded.filter { !it.nullable }
    if (nonnull.isNotEmpty())
      throw IllegalArgumentException("Cannot evolve schema when new field(s) $nonnull are not nullable")

    if (toBeAdded.isNotEmpty()) {
      val table = client.getTable(dbName.value, tableName.value)
      toBeAdded.forEach {
        val col = ToHiveSchema.toHiveSchema(it)
        table.sd.cols.add(col)
      }
      client.alter_table(dbName.value, tableName.value, table)
    }

    val table = client.getTable(dbName.value, tableName.value)
    return FromHiveSchema.fromHiveTable(table)
  }
}