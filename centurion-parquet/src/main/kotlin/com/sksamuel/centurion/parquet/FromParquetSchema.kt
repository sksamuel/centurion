package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.ArrayType
import com.sksamuel.centurion.BinaryType
import com.sksamuel.centurion.BooleanType
import com.sksamuel.centurion.DateType
import com.sksamuel.centurion.DecimalType
import com.sksamuel.centurion.EnumType
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
import com.sksamuel.centurion.TimeMillisType
import com.sksamuel.centurion.TimestampMillisType
import com.sksamuel.centurion.Type
import com.sksamuel.centurion.VarcharType
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType

/**
 * Conversion functions from parquet types to centurion types.
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
      OriginalType.TIME_MILLIS -> TimeMillisType
      else -> Int32Type
    }

    fun int64(original: OriginalType?): Type = when (original) {
      OriginalType.UINT_64 -> Int64Type
      OriginalType.INT_64 -> Int64Type
      OriginalType.TIMESTAMP_MILLIS -> TimestampMillisType
      else -> Int64Type
    }

    fun binary(type: PrimitiveType, original: OriginalType?, length: Int): Type = when (original) {
      OriginalType.ENUM -> EnumType(emptyList())
      OriginalType.UTF8 -> if (length > 0) VarcharType(length) else StringType
      OriginalType.DECIMAL -> {
        val meta = type.decimalMetadata
        DecimalType(Precision(meta.precision), Scale(meta.scale))
      }
      else -> BinaryType
    }

    val elementType = when (type.primitiveTypeName!!) {
      PrimitiveType.PrimitiveTypeName.BINARY -> binary(type, type.originalType, type.typeLength)
      PrimitiveType.PrimitiveTypeName.BOOLEAN -> BooleanType
      PrimitiveType.PrimitiveTypeName.DOUBLE -> Float64Type
      PrimitiveType.PrimitiveTypeName.FLOAT -> Float32Type
      PrimitiveType.PrimitiveTypeName.INT32 -> int32(type.originalType)
      PrimitiveType.PrimitiveTypeName.INT64 -> int64(type.originalType)
      // INT96 is deprecated, but it's commonly used (wrongly) for timestamp millis
      // Spark does this, so we must do too
      // https://issues.apache.org/jira/browse/PARQUET-323
      PrimitiveType.PrimitiveTypeName.INT96 -> TimestampMillisType
      PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> binary(type, type.originalType, type.typeLength)
    }

    return if (type.isRepeated()) ArrayType(elementType) else elementType
  }
}

private fun org.apache.parquet.schema.Type.Repetition.isNullable(): Boolean =
    this == org.apache.parquet.schema.Type.Repetition.OPTIONAL

private fun PrimitiveType.isRepeated(): Boolean {
  return repetition == org.apache.parquet.schema.Type.Repetition.REPEATED
}
