package com.sksamuel.reactivehive

import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * Normally, you would pre-process your structs so that they are in the format
 * that you want to store. For example, if you have payment data where you didn't
 * want to persist the credit card number, then as part of your stream pipeline
 * you would perform a mapping operation (projection in SQL parlance) to remove
 * this unwanted field before the struct is passed to the reactive hive sink.
 *
 * However, the hive metastore schema may not match the schema passed to reactive-hive.
 * For example, your pipeline may have changed to include a new field that wasn't
 * present in older data, or you may have renamed a field and want to use the new
 * name in an existing table. Or you may want to take a struct and have reactive-hive
 * drop any fields that are not in the metastore schema.
 *
 * You can of course change the metastore schema externally through some tool
 * like impala, spark, or the hive CLI. However, through implements of this interface,
 * reactive-hive can handle all these scenarios for you.
 *
 * For example, you may wish to update the metastore schema so that it has
 * new missing fields added (schema evolution), or you may wish to throw an exception
 * if the schemas do not match, or you may wish to ignore any mismatches, or
 * you may wish to keep only compatible fields, and so on.
 *
 */
interface StructAlignment {
  fun align(dbName: DatabaseName,
            tableName: TableName,
            metastoreSchema: StructType,
            struct: Struct,
            client: IMetaStoreClient): Struct
}

/**
 * An implementation of [StructAlignment] that will update the metastore by
 * attempting to add new fields in a backwards compatible way.
 *
 * This can be accomplished if the new field is nullable (so that existing data
 * can return null for this field when queried).
 *
 * If any fields are missing from the metastore schema, but are not nullable,
 * then an exception will be thrown.
 */
class AdditiveEvolutionStructAlignment : StructAlignment {

  override fun align(dbName: DatabaseName,
                     tableName: TableName,
                     metastoreSchema: StructType,
                     struct: Struct,
                     client: IMetaStoreClient): Struct {

    // find any fields that are missing from the metastore
    val missing = struct.schema.fields.filter { metastoreSchema.hasField(it.name) }
    val nonnull = missing.filter { !it.nullable }
    if (nonnull.isNotEmpty())
      throw IllegalArgumentException("Cannot evolve schema when new field(s) $nonnull are not nullable")

    if (missing.isNotEmpty()) {
      val table = client.getTable(dbName.value, tableName.value)
      missing.forEach {
        val col = ToHiveSchema.toHiveSchema(it)
        table.sd.cols.add(col)
      }
      client.alter_table(dbName.value, tableName.value, table)
    }

    // any fields that are in the metastore but not in the input struct throw an error
    if (struct.schema.fields.any { !metastoreSchema.hasField(it.name) })
      throw RuntimeException()

    return struct
  }
}

/**
 * An implementation of [StructAlignment] that is a passthrough operation.
 *
 * Using this implementation means reactive-hive will make no changes
 * to the metastore or the struct. The schema must be managed externally.
 *
 * This implementation can result in structs that are not compatible
 * with the metastore. Use of this implementation is NOT recommended in
 * production.
 */
object NoopStructAlignment : StructAlignment {
  override fun align(dbName: DatabaseName,
                     tableName: TableName,
                     metastoreSchema: StructType,
                     struct: Struct,
                     client: IMetaStoreClient): Struct = struct
}

/**
 * An implementation of [StructAlignment] that will return a new struct
 * that is compatible with the metastore schema. This means that extra fields
 * will be dropped, and missing fields will be padded (if nullable).
 *
 * In other words, if a metastore has schema { a, c } and a struct has
 * schema { b, c, d } then fields { b, d } will be removed from the
 * incoming struct so that it matches the metastore. Field { a } will
 * then be added with a value of null.
 *
 * Any fields in the metastore that are not present in the struct must
 * be marked as nullable or an error will be thrown.
 */
object CompatibleStructAligner : StructAlignment {

  override fun align(dbName: DatabaseName,
                     tableName: TableName,
                     metastoreSchema: StructType,
                     struct: Struct,
                     client: IMetaStoreClient): Struct {

    val values = metastoreSchema.fields.map {
      val value = struct[it.name]
      value ?: {
        if (it.nullable) null else throw IllegalStateException("Field ${it.name} is missing from input and not nullable so cannot be padded")
      }
    }

    return Struct(metastoreSchema, values)
  }
}

/**
 * An implementation of [StructAlignment] that requires the fields
 * in the incoming struct's schema to be the same as the fields in
 * the hive metastore. The order of fields is not important, but the names
 * and types are.
 *
 * Any missing fields or extra fields will throw an exception.
 *
 * This is a good choice if you must ensure that incoming data matches
 * the metastore exactly.
 *
 * Using this implementation means reactive-hive will make no changes
 * to the metastore. Instead, the schema must be managed externally.
 */
object StrictStructAligner : StructAlignment {

  override fun align(dbName: DatabaseName,
                     tableName: TableName,
                     metastoreSchema: StructType,
                     struct: Struct,
                     client: IMetaStoreClient): Struct {

    val values = metastoreSchema.fields.map {
      if (struct.schema[it.name] == null)
        throw RuntimeException("Field ${it.name} is missing in input struct")
      if (struct.schema[it.name]?.type != it.type)
        throw RuntimeException("Field ${it.name} has different type in input ${struct.schema[it.name]?.type} to metastore ${it.type}}")
      struct[it.name]
    }

    return Struct(metastoreSchema, values)
  }
}