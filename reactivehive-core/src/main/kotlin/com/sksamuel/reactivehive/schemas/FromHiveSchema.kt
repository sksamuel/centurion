package com.sksamuel.reactivehive.schemas

import arrow.core.Option
import arrow.core.getOrElse
import arrow.core.orElse
import com.sksamuel.reactivehive.ArrayType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.CharType
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.DecimalType
import com.sksamuel.reactivehive.Float32Type
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.Int16Type
import com.sksamuel.reactivehive.Int64Type
import com.sksamuel.reactivehive.Int8Type
import com.sksamuel.reactivehive.Precision
import com.sksamuel.reactivehive.Scale
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.Type
import com.sksamuel.reactivehive.VarcharType
import org.apache.hadoop.hive.metastore.api.FieldSchema

object FromHiveSchema {

  object HiveRegexes {
    val decimal_r = "decimal\\(\\s*(\\d+?)\\s*,\\s*(\\d+?)\\s*\\)".toRegex()
    val array_r = "array<(.+?)>".toRegex()
    val char_r = "char\\(\\s*(\\d+?)\\s*\\)".toRegex()
    val struct_r = "struct<(.+?)>".toRegex()
    val struct_field_r = "(.+?):(.+?)(,|$)".toRegex()
    val varchar_r = "varchar\\(\\s*(\\d+?)\\s*\\)".toRegex()
  }

  fun fromHiveSchema(schema: FieldSchema): Type = fromHiveType(
      schema.type)

  fun fromHiveType(type: String): Type {

    fun decimal(): Option<DecimalType> {
      val match = FromHiveSchema.HiveRegexes.decimal_r.matchEntire(type)
      return Option.fromNullable(match).map {
        val p = Precision(it.groupValues[1].toInt())
        val s = Scale(it.groupValues[2].toInt())
        DecimalType(p, s)
      }
    }

    fun varchar(): Option<VarcharType> {
      val match = FromHiveSchema.HiveRegexes.varchar_r.matchEntire(type)
      return Option.fromNullable(match).map {
        val size = it.groupValues[1].toInt()
        VarcharType(size)
      }
    }

    fun char(): Option<CharType> {
      val match = FromHiveSchema.HiveRegexes.char_r.matchEntire(type)
      return Option.fromNullable(match).map {
        val size = it.groupValues[1].toInt()
        CharType(size)
      }
    }

    fun struct(): Option<StructType> {
      val match = FromHiveSchema.HiveRegexes.struct_r.matchEntire(type)
      return Option.fromNullable(match).map { structMatch ->
        val fields = FromHiveSchema.HiveRegexes.struct_field_r.findAll(structMatch.groupValues[1].trim()).map { fieldMatch ->
          StructField(fieldMatch.groupValues[1].trim(),
              fromHiveType(fieldMatch.groupValues[2].trim()))
        }
        StructType(fields.toList())
      }
    }

    fun array(): Option<ArrayType> {
      val match = FromHiveSchema.HiveRegexes.array_r.matchEntire(type)
      return Option.fromNullable(match).map {
        val elementType = fromHiveType(it.groupValues[1].trim())
        ArrayType(elementType)
      }
    }

    return when (type) {
      HiveTypes.string -> StringType
      HiveTypes.float -> Float32Type
      HiveTypes.double -> Float64Type
      HiveTypes.bigint -> Int64Type
      HiveTypes.boolean -> BooleanType
      HiveTypes.smallint -> Int16Type
      HiveTypes.tinyint -> Int8Type
      HiveTypes.date -> DateType
      else -> {
        decimal()
            .orElse { varchar() }
            .orElse { array() }
            .orElse { char() }
            .orElse { struct() }
            .getOrElse { throw UnsupportedOperationException("Unknown hive type $type") }
      }
    }
  }
}