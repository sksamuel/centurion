package com.sksamuel.rxhive.schemas

import com.sksamuel.rxhive.ArrayType
import com.sksamuel.rxhive.BigIntType
import com.sksamuel.rxhive.BinaryType
import com.sksamuel.rxhive.BooleanType
import com.sksamuel.rxhive.CharType
import com.sksamuel.rxhive.DateType
import com.sksamuel.rxhive.DecimalType
import com.sksamuel.rxhive.EnumType
import com.sksamuel.rxhive.Float32Type
import com.sksamuel.rxhive.Float64Type
import com.sksamuel.rxhive.Int16Type
import com.sksamuel.rxhive.Int32Type
import com.sksamuel.rxhive.Int64Type
import com.sksamuel.rxhive.Int8Type
import com.sksamuel.rxhive.MapDataType
import com.sksamuel.rxhive.Precision
import com.sksamuel.rxhive.Scale
import com.sksamuel.rxhive.StringType
import com.sksamuel.rxhive.StructField
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TimeMicrosType
import com.sksamuel.rxhive.TimeMillisType
import com.sksamuel.rxhive.TimestampMicrosType
import com.sksamuel.rxhive.TimestampMillisType
import com.sksamuel.rxhive.Type
import com.sksamuel.rxhive.VarcharType
import org.apache.hadoop.hive.metastore.api.FieldSchema

object ToHiveSchema {

  /**
   *  Numeric Types
   *
   *  TINYINT (1-byte signed integer, from -128 to 127)
   *  SMALLINT (2-byte signed integer, from -32,768 to 32,767)
   *  INT/INTEGER (4-byte signed integer, from -2,147,483,648 to 2,147,483,647)
   *  BIGINT (8-byte signed integer, from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
   *  FLOAT (4-byte single precision floating point number)
   *  DOUBLE (8-byte double precision floating point number)
   *  DOUBLE PRECISION (alias for DOUBLE, only available starting with Hive 2.2.0)
   *  DECIMAL
   *  Introduced in Hive 0.11.0 with a precision of 38 digits
   *  Hive 0.13.0 introduced user-definable precision and scale
   *  NUMERIC (same as DECIMAL, starting with Hive 3.0.0)
   *
   *  Hive types are taken from the language spec at https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
   */
  object HiveTypeFns {

    fun struct(struct: StructType) = "struct<${struct.fields.joinToString(",") {
      structfield(it)
    }}>"
    private fun structfield(field: StructField) = "${field.name}:${toHiveType(
        field.type)}"

    fun varchar(size: Int) = "varchar($size)"
    fun char(size: Int) = "char($size)"

    /**
     * The DECIMAL type in Hive is based on Java's BigDecimal which is used for representing immutable arbitrary
     * precision decimal numbers in Java.
     * As of Hive 0.13 users can specify scale and precision when creating tables with the DECIMAL datatype
     * using a DECIMAL(precision, scale) syntax.
     * If scale is not specified, it defaults to 0 (no fractional digits).
     * If no precision is specified, it defaults to 10.
     */
    fun decimal(precision: Precision, scale: Scale) = "decimal(${precision.value},${scale.value})"

    fun array(elementType: Type) = "array<${toHiveType(
        elementType)}>"
  }

  fun toHiveType(type: Type): String {
    return when (type) {
      is StructType -> ToHiveSchema.HiveTypeFns.struct(type)
      BooleanType -> HiveTypes.boolean
      BinaryType -> HiveTypes.binary
      StringType -> HiveTypes.string
      is CharType -> ToHiveSchema.HiveTypeFns.char(type.size)
      is VarcharType -> ToHiveSchema.HiveTypeFns.varchar(
          type.size)
      Float32Type -> HiveTypes.float
      Float64Type -> HiveTypes.double
      Int8Type -> HiveTypes.tinyint
      Int16Type -> HiveTypes.smallint
      Int32Type -> HiveTypes.int
      Int64Type -> HiveTypes.bigint
      BigIntType -> TODO()
      TimestampMillisType -> HiveTypes.timestamp
      TimestampMicrosType -> TODO()
      TimeMicrosType -> TODO()
      TimeMillisType -> TODO()
      DateType -> HiveTypes.date
      is MapDataType -> TODO()
      is EnumType -> TODO()
      is ArrayType -> ToHiveSchema.HiveTypeFns.array(type.elementType)
      is DecimalType -> ToHiveSchema.HiveTypeFns.decimal(
          type.precision,
          type.scale)
    }
  }

  fun toHiveSchema(field: StructField): FieldSchema = FieldSchema(field.name,
      toHiveType(field.type), null)

  fun toHiveSchema(schema: StructType): List<FieldSchema> {
    return schema.fields.map {
      FieldSchema(it.name, toHiveType(it.type), null)
    }
  }
}