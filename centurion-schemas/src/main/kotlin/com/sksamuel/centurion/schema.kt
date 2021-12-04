package com.sksamuel.centurion

sealed interface Schema {

  sealed interface Primitive : Schema

  // // string types

  object Strings : Primitive
  data class Varchar(val length: Int) : Primitive

  object Booleans : Primitive
  object Bytes : Primitive

  object Nulls : Primitive

  //// integral types

  object Int64 : Primitive
  object Int32 : Primitive
  object Int16 : Primitive
  object Int8 : Primitive

  // floating point types

  object Float64 : Primitive
  object Float32 : Primitive

  // date types

  // timestamp as milliseconds since epoch
  object TimestampMillis : Primitive

  //// decimal types

  data class Precision(val value: Int)
  data class Scale(val value: Int)

  data class DecimalType(val precision: Precision, val scale: Scale) : Primitive

  // a nullable type wraps any other type, denoting that it is permitted to be null
  // this is analogous to Avro's union type, with two elements - null and another
  data class Nullable(val element: Schema) : Schema

  data class Enum(val symbols: List<String>) : Schema {
    constructor(vararg values: String) : this(values.asList())
  }

  data class Struct(val name: String, val fields: List<Field>) : Schema {
    constructor(name: String, vararg fields: Field) : this(name, fields.toList())

    init {
      require(fields.map { it.name }.distinct().size == fields.size) { "Record cannot contain duplicated field names" }
    }
    fun indexOf(name: String): Int = fields.indexOfFirst { it.name == name }
  }

  data class Array(val elements: Schema) : Schema

  // always String keys
  data class Map(val values: Schema) : Schema

  data class Field(val name: String, val schema: Schema)
}

fun Schema.nullable(): Schema = if (this is Schema.Nullable) this else Schema.Nullable(this)
