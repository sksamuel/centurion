package com.sksamuel.rxhive.processors

import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructType

/**
 * An implementation of [StructTransformer] that will align an input
 * record so that it's structure matches that defined in the metastore.
 *
 * Sometimes an input record will not contain a value for a field defined
 * by the metastore schema. If the metastore field has a default value
 * or allows nulls, then output record could be padded to include that field.
 *
 * Secondly, the input record may specify extra fields which are not required
 * by the hive table. Therefore these records can be safely dropped.
 *
 * Lastly, for file formats that do not include field information, such as
 * CSV without headers, the field orderings must match the hive metastore.
 * An input record may include all the required fields but in a different
 * order, and so this transformer can reorder the fields as required.
 */
class MetastoreSchemaStructTransformer(val schema: StructType) : StructTransformer {
  override fun process(struct: Struct): Struct = TODO()
//    schema.fields.foldLeft(Struct(schema)) {
//      Try(struct.get(field.name)).toOption match {
//        Some(value) -> struct.put(field.name, value)
//        None if field.schema.isOptional => struct.put(field.name, null)
//        None -> sys . error (s"Cannot map struct to required schema; ${field.name} is missing, no default value has been supplied and null is not permitted")
//      }
//    }
}