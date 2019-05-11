package com.sksamuel.reactivehive

/**
 * Types are inspired by Apache Spark.
 * https://github.com/apache/spark/blob/630e25e35506c02a0b1e202ef82b1b0f69e50966/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DataType.scala
 */
sealed class Type

data class StructType(val fields: List<StructField>) : Type() {

  constructor(vararg fields: StructField) : this(fields.toList())

  fun indexOf(name: String): Int = fields.indexOfFirst { it.name == name }

  operator fun get(index: Int): StructField = fields[index]

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

/**
 * @param nullable whether this struct field can be null or not. Defaults to true as that
 * is the behaviour of hive
 */
data class StructField(val name: String,
                       val type: Type,
                       val nullable: Boolean = true) {

  /**
   * Returns a new StructField with the same name as this field, but lowercased.
   * All other properties remain the same.
   */
  fun toLowerCase(): StructField = copy(name = name.toLowerCase())

  fun withNullable(nullable: Boolean): StructField = copy(nullable = nullable)

  fun nullable(): StructField = copy(nullable = true)
}

data class Struct(val schema: StructType, val values: List<Any?>) {
  constructor(type: StructType, vararg values: Any?) : this(type, values.asList())

  companion object {
    fun fromMap(schema: StructType, map: Map<String, Any?>): Struct {
      val values = schema.fields.map { map[it.name] }
      return Struct(schema, values)
    }
  }
}

object BooleanType : Type()
object BinaryType : Type()

// text types
object StringType : Type()

data class CharDataType(val size: Int) : Type()
data class VarcharDataType(val size: Int) : Type()

// floating point types
object Float64Type : Type()

object Float32Type : Type()

// integral types
object Int8Type : Type()
object Int16Type : Type()
object Int32Type : Type()
object Int64Type : Type()
object BigIntType : Type()

// date time types
object TimestampMillisType : Type()
object TimestampMicrosType : Type()
object TimeMicrosType : Type()
object TimeMillisType : Type()
object DateType : Type()

// container types
data class MapDataType(val keyType: Type, val valueType: Type) : Type()

data class EnumType(val values: List<String>) : Type()

data class ArrayType(val elementType: Type) : Type()

data class Precision(val value: Int)
data class Scale(val value: Int)
data class DecimalType(val precision: Precision, val scale: Scale) : Type()
