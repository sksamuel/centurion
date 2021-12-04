package com.sksamuel.centurion.schemas

import arrow.core.Option
import arrow.core.getOrElse
import arrow.core.orElse
import com.sksamuel.centurion.ArrayType
import com.sksamuel.centurion.BooleanType
import com.sksamuel.centurion.CharType
import com.sksamuel.centurion.DateType
import com.sksamuel.centurion.DecimalType
import com.sksamuel.centurion.Float32Type
import com.sksamuel.centurion.Float64Type
import com.sksamuel.centurion.Int16Type
import com.sksamuel.centurion.Int32Type
import com.sksamuel.centurion.Int64Type
import com.sksamuel.centurion.Int8Type
import com.sksamuel.centurion.Precision
import com.sksamuel.centurion.Scale
import com.sksamuel.centurion.StringType
import com.sksamuel.centurion.StructField
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.Type
import com.sksamuel.centurion.VarcharType
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.Table

object FromHiveSchema {

  object HiveRegexes {
    val decimal_r = "decimal\\(\\s*(\\d+?)\\s*,\\s*(\\d+?)\\s*\\)".toRegex()
    val array_r = "array<(.+?)>".toRegex()
    val char_r = "char\\(\\s*(\\d+?)\\s*\\)".toRegex()
    val struct_r = "struct<(.+?)>".toRegex()
    val struct_field_r = "(.+?):(.+?)(,|$)".toRegex()
    val varchar_r = "varchar\\(\\s*(\\d+?)\\s*\\)".toRegex()
  }

  fun fromHiveTable(table: Table): StructType {
    // in hive prior to 3.0, columns are null, partitions non-null
    val fields = table.sd.cols.map {
      StructField(it.name, fromFieldSchema(it), true)
    } + table.partitionKeys.map {
      StructField(it.name, fromFieldSchema(it), false)
    }
    return StructType(fields)
  }

  fun fromFieldSchema(schema: FieldSchema): Type = fromHiveType(
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
      HiveTypes.boolean -> BooleanType
      HiveTypes.double -> Float64Type
      HiveTypes.float -> Float32Type
      HiveTypes.bigint -> Int64Type
      HiveTypes.int -> Int32Type
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
