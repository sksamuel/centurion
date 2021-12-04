package com.sksamuel.centurion

data class Struct(val schema: Schema.Struct, val values: List<Any?>) {
  constructor(schema: Schema.Struct, vararg values: Any?) : this(schema, values.asList())

  init {
    require(schema.fields.size == values.size)
  }

  private val names by lazy { schema.fields.map { it.name } }

  /**
   * Return the value of the record for the given [fieldName].
   * Will throw an error if the field is not defined in the schema.
   */
  operator fun get(fieldName: String): Any? {
    val index = names.indexOf(fieldName)
    if (index < 0) error("Field $fieldName does not exist in schema $schema")
    return values[index]
  }
}

class StructBuilder(val schema: Schema.Struct) {

  private val values = Array<Any?>(schema.fields.size) { null }

  operator fun set(fieldName: String, value: Any?) {
    values[schema.indexOf(fieldName)] = value
  }

  fun clear() {
    values.fill(null)
  }

  fun toStruct(): Struct = Struct(schema, values.toList())
}
