package com.sksamuel.reactivehive

/**
 * Returns a new [Struct] which has the fields of the given schema.
 * Any extra fields in the struct will be dropped, and any missing
 * fields will be padded if possible (or an exception will be thrown)
 */
fun align(struct: Struct, schema: StructType): Struct {

  val value = schema.fields.map {
    val value = struct[it.name]
    value ?: {
      if (it.nullable) null else throw IllegalStateException("Field ${it.name} is missing from input and not nullable so cannot be padded")
    }
  }

  return Struct(schema, value)
}