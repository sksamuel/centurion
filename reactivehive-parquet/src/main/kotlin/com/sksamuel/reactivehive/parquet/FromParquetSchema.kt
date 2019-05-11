package com.sksamuel.reactivehive.parquet

import com.sksamuel.reactivehive.ArrayType
import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.Int8Type
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.EnumType
import com.sksamuel.reactivehive.Float32Type
import com.sksamuel.reactivehive.Int32Type
import com.sksamuel.reactivehive.Int64Type
import com.sksamuel.reactivehive.Int16Type
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TimestampMillisType
import com.sksamuel.reactivehive.Type
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType

/**
 * Conversion functions from parquet types to reactive-hive types.
 *
 * Parquet types are defined at the parquet repo:
 * https://github.com/apache/parquet-format/blob/c6d306daad4910d21927b8b4447dc6e9fae6c714/LogicalTypes.md
 */
object FromParquetSchema {

  fun fromParquet(type: org.apache.parquet.schema.Type): Type {
    return when (type) {
      is PrimitiveType -> fromPrimitiveType(type)
      is MessageType -> fromGroupType(type)
      is GroupType -> fromGroupType(type)
      else -> throw UnsupportedOperationException()
    }
  }

  fun fromGroupType(groupType: GroupType): StructType {
    val fields = groupType.fields.map {
      val fieldType = fromParquet(it)
      StructField(
          it.name,
          fieldType,
          it.repetition.isNullable()
      )
    }
    return StructType(fields)
  }

  fun fromPrimitiveType(type: PrimitiveType): Type {

    fun int32(original: OriginalType?): Type = when (original) {
      OriginalType.UINT_32 -> Int32Type
      OriginalType.INT_32 -> Int32Type
      OriginalType.UINT_16 -> Int16Type
      OriginalType.INT_16 -> Int16Type
      OriginalType.UINT_8 -> Int8Type
      OriginalType.INT_8 -> Int8Type
      OriginalType.DATE -> DateType
      else -> Int32Type
    }

    fun int64(original: OriginalType?): Type = when (original) {
      OriginalType.UINT_64 -> Int64Type
      OriginalType.INT_64 -> Int64Type
      OriginalType.TIMESTAMP_MILLIS -> TimestampMillisType
      else -> Int64Type
    }

    fun binary(original: OriginalType?, length: Int): Type = when (original) {
      OriginalType.ENUM -> EnumType(emptyList())
      OriginalType.UTF8 -> StringType
      else -> BinaryType
    }

    val elementType = when (type.primitiveTypeName) {
      PrimitiveType.PrimitiveTypeName.BINARY -> binary(type.originalType, type.typeLength)
      PrimitiveType.PrimitiveTypeName.BOOLEAN -> BooleanType
      PrimitiveType.PrimitiveTypeName.DOUBLE -> Float64Type
      PrimitiveType.PrimitiveTypeName.FLOAT -> Float32Type
      PrimitiveType.PrimitiveTypeName.INT32 -> int32(type.originalType)
      PrimitiveType.PrimitiveTypeName.INT64 -> int64(type.originalType)
      PrimitiveType.PrimitiveTypeName.INT96 -> TimestampMillisType
      else -> throw java.lang.UnsupportedOperationException("Unsupported data type ${type.primitiveTypeName}")
    }

    return if (type.isRepeated()) ArrayType(elementType) else elementType
  }
}

private fun org.apache.parquet.schema.Type.Repetition.isNullable(): Boolean =
    this == org.apache.parquet.schema.Type.Repetition.OPTIONAL

private fun PrimitiveType.isRepeated(): Boolean {
  return repetition == org.apache.parquet.schema.Type.Repetition.REPEATED
}