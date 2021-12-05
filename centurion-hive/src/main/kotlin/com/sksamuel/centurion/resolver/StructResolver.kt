package com.sksamuel.centurion.resolver

import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.align

interface StructResolver {
  fun resolve(struct: Struct, metastoreSchema: StructType): Struct
}

object NoopStructResolver : StructResolver {
  override fun resolve(struct: Struct, metastoreSchema: StructType): Struct = struct
}

/**
 * An implementation of [StructResolver] that will return a struct that is aligned
 * with the metastore schema by dropping fields present in the struct but not
 * present in the metastore, and padding fields in the metastore but not present
 * in the struct.
 *
 * In other words, if a metastore has schema { a, c } and a struct has
 * schema { b, c, d } then fields { b, d } will be removed from the
 * incoming struct so that it matches the metastore. Field { a } will
 * then be added with a value of null.
 *
 * Any fields in the metastore that are not present in the struct must
 * be marked as nullable or an error will be thrown.
 */
object LenientStructResolver : StructResolver {
  override fun resolve(struct: Struct, metastoreSchema: StructType): Struct {
    return align(struct, metastoreSchema)
  }
}

object StrictStructResolver : StructResolver {
  override fun resolve(struct: Struct, metastoreSchema: StructType): Struct {

    val a = metastoreSchema.fields.map { it.name to it.type }.toSet()
    val b = struct.schema.fields.map { it.name to it.type }.toSet()

    if (a != b) {
      throw RuntimeException("Metastore schema and struct schema are not equal $a != $b")
    }

    return struct
  }
}
