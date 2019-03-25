package com.sksamuel.reactivehive

/**
 * Schemas are inspired by Apache Spark.
 * https://github.com/apache/spark/blob/630e25e35506c02a0b1e202ef82b1b0f69e50966/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DataType.scala
 */

// all schema types are a subclass of DataType
sealed class DataType {
  /** Readable string representation for the type. */
  abstract fun name(): String
}

object BooleanDataType : DataType() {
  override fun name(): String = "boolean"
}

object BinaryDataType : DataType() {
  override fun name(): String = "binary"
}

// string types

object StringDataType : DataType() {
  override fun name(): String = "string"
}

data class CharDataType(val size: Int) : DataType() {
  override fun name(): String = "char($size)"
}

data class VarcharDataType(val size: Int) : DataType() {
  override fun name(): String = "varchar($size)"
}

data class EnumDataType(val values: List<String>) : DataType() {
  override fun name(): String = "enum"
}

// - date types

object DateDataType : DataType() {
  override fun name(): String = "date"
}

object TimestampDataType : DataType() {
  override fun name(): String = "timestamp"
}

// -- number types

object BigIntDataType : DataType() {
  override fun name(): String = "bigint"
}

// 32 bit floating
object FloatDataType : DataType() {
  override fun name(): String = "float"
}

// 64 bit floating
object DoubleDataType : DataType() {
  override fun name(): String = "double"
}

// 8 bit number
object UTinyIntDataType : DataType() {
  override fun name(): String = "utinyint"
}

object TinyIntDataType : DataType() {
  override fun name(): String = "tinyint"
}

// 16 bit number
object USmallIntDataType : DataType() {
  override fun name(): String = "usmallint"
}

object SmallIntDataType : DataType() {
  override fun name(): String = "smallint"
}

// 32 bit number
object UIntDataType : DataType() {
  override fun name(): String = "uint"
}

object IntDataType : DataType() {
  override fun name(): String = "int"
}

// 64 bit number
object ULongDataType : DataType() {
  override fun name(): String = "ulong"
}

object LongDataType : DataType() {
  override fun name(): String = "long"
}

data class Precision(val value: Int)
data class Scale(val value: Int)

data class DecimalDataType(val precision: Precision, val scale: Scale) : DataType() {
  override fun name(): String = "decimal<${precision.value}, ${scale.value}>"
}

// -- complex/struct types
data class ArrayDataType(val elementType: DataType) : DataType() {
  override fun name(): String = "array"
}

data class MapDataType(val keyType: DataType, val valueType: DataType) : DataType() {
  override fun name(): String = "map"
}

data class StructType(val fields: List<StructField>) : DataType() {
  override fun name(): String = "struct"

  constructor(vararg fields: StructField) : this(fields.toList())

  fun indexOf(name: String): Int = fields.indexOfFirst { it.name == name }

  fun fieldAt(index: Int): StructField = fields[index]

  /**
   * Returns a new StructType which is the same as the existing struct, but with the
   * given field added to the end of the existing fields.
   */
  fun addField(field: StructField): StructType {
    require(!fields.any { it.name == field.name }) { "Field ${field.name} already exists" }
    return copy(fields = this.fields + field)
  }

  /**
   * Returns a new [StructType] which is the same as the existing struct, but with
   * the matching field removed, if it exists.
   *
   * If the field does not exist then it is a no-op.
   */
  fun removeField(name: String): StructType {
    return StructType(fields.filterNot { it.name == name })
  }
}

data class StructField(val name: String,
                       val type: DataType,
                       val nullable: Boolean = true) {

  /**
   * Returns a new StructField with the same name as this field, but lowercased.
   * All other properties rename the same.
   */
  fun toLowerCase(): StructField = copy(name = name.toLowerCase())

  fun withNullable(nullable: Boolean): StructField = copy(nullable = nullable)
}