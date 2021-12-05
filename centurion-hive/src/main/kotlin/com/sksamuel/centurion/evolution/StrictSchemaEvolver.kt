package com.sksamuel.centurion.evolution

import com.sksamuel.centurion.DatabaseName
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.TableName
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementation of [SchemaEvolver] that requires the schema in the incoming
 * struct to be equal to the schema in the hive metastore.
 *
 * Equality is defined as the same number of fields, with the same names and
 * same types. Order is not important.
 *
 * Any missing fields or extra fields will throw an exception.
 *
 * This is a good choice if you must ensure that incoming data matches
 * the metastore schema exactly.
 *
 * This implementation will never make any changes to the metastore.
 */
object StrictSchemaEvolver : SchemaEvolver {

  override fun evolve(dbName: DatabaseName,
                      tableName: TableName,
                      metastoreSchema: StructType,
                      struct: Struct,
                      client: IMetaStoreClient): StructType {

    val a = metastoreSchema.fields.map { it.name to it.type }.toSet()
    val b = struct.schema.fields.map { it.name to it.type }.toSet()

    if (a != b) {
      throw RuntimeException("Metastore schema and struct schema are not equal $a != $b")
    }

    return metastoreSchema
  }
}
