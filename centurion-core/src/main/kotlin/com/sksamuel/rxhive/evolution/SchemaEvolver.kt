package com.sksamuel.rxhive.evolution

import com.sksamuel.rxhive.DatabaseName
import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TableName
import org.apache.hadoop.hive.metastore.IMetaStoreClient

/**
 * Implementations of [SchemaEvolver] ensure that incoming [Struct] records have a schema
 * that is compatible with the schema in the hive metastore. It does this by either changing
 * the schema in the metastore, or returning an updated struct, or a combination of the two
 * (or neither if you prefer! - see [NoopSchemaEvolver]).
 *
 * Normally, before a struct is passed to rxhive it would have been pre-processed
 * in some way so that the schema is in the format that should be persisted.
 *
 * For example, if you have payment data where you didn't want to persist the credit card
 * number, then as part of your stream pipeline you would perform a transformation operation
 * (projection in SQL parlance) to remove any unwanted fields before the struct is passed
 * to the rxhive component.
 *
 * However, the schema in the hive metastore may not match the schema passed to rxhive.
 * For example, your pipeline may have changed to include a new field that wasn't
 * present when the table was created, or you may have renamed a field and want to use the new
 * name in an existing table. Or you may have many free-form structs and you want any
 * extranous fields to be dropped.
 *
 * You can of course change the metastore schema externally through some tool
 * like impala, spark, or the hive CLI. However, through implements of this interface,
 * rxhive can handle all these scenarios for you.
 *
 * For example, you may wish to update the metastore schema so that it has
 * new missing fields added (schema evolution), or you may wish to throw an exception
 * if the schemas do not match, you may wish to ignore any mismatches, or
 * you may wish to keep only compatible fields, and so on.
 *
 */
interface SchemaEvolver {

  fun evolve(dbName: DatabaseName,
             tableName: TableName,
             metastoreSchema: StructType,
             struct: Struct,
             client: IMetaStoreClient): StructType
}