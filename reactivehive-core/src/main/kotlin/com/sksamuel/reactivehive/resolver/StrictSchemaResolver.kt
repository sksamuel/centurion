package com.sksamuel.reactivehive.resolver

import arrow.core.Tuple2
import com.sksamuel.reactivehive.DatabaseName
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TableName
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * An implementation of [SchemaResolver] that requires the schema in the incoming
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
object StrictSchemaResolver : SchemaResolver {

  override fun align(dbName: DatabaseName,
                     tableName: TableName,
                     metastoreSchema: StructType,
                     struct: Struct,
                     client: IMetaStoreClient): Tuple2<Boolean, Struct> {

    val values = metastoreSchema.fields.map {
      if (struct.schema[it.name] == null)
        throw RuntimeException("Field ${it.name} is missing in input struct")
      if (struct.schema[it.name]?.type != it.type)
        throw RuntimeException("Field ${it.name} has different type in input ${struct.schema[it.name]?.type} to metastore ${it.type}}")
      struct[it.name]
    }

    return Tuple2(false, Struct(metastoreSchema, values))
  }
}