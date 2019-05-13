package com.sksamuel.reactivehive.resolver

import arrow.core.Tuple2
import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TableName
import com.sksamuel.reactivehive.align
import com.sksamuel.reactivehive.schemas.FromHiveSchema
import com.sksamuel.reactivehive.schemas.ToHiveSchema
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementation of [SchemaResolver] that will update the metastore by
 * attempting to add new fields in a backwards compatible way.
 *
 * This can be accomplished if the new field is nullable (so that existing data
 * can return null for this field when queried).
 *
 * If any fields are missing from the metastore schema, but are not nullable,
 * then an exception will be thrown.
 *
 * @param pad if true then fields in the metastore but not in the struct will be added
 * to the struct with null (if they are nullable). If false, then an exception will be
 * throw if the input struct is missing any metastore fields.
 */
class SchemaEvolutionSchemaResolver(val pad: Boolean) : SchemaResolver {

  override fun align(dbName: DatabaseName,
                     tableName: TableName,
                     metastoreSchema: StructType,
                     struct: Struct,
                     client: IMetaStoreClient): Tuple2<Boolean, Struct> {

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

    // now the metastore has at least all the fields the struct does, but the struct may be
    // missing fields that the metastore does. We can fetch the schema from the metastore again
    // to get the latest, and then align against that.
    val table = client.getTable(dbName.value, tableName.value)
    val updatedMetastoreSchema = FromHiveSchema.fromHiveTable(table)

    val aligned = align(struct, updatedMetastoreSchema)

    return Tuple2(toBeAdded.isNotEmpty(), aligned)
  }
}