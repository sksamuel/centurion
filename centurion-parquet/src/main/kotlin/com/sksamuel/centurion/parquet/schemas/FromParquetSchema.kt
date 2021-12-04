package com.sksamuel.centurion.parquet.schemas

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.nullable
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type

/**
 * Conversion functions from parquet types to centurion types.
 *
 * Parquet types are defined at the parquet repo:
 * https://github.com/apache/parquet-format/blob/c6d306daad4910d21927b8b4447dc6e9fae6c714/LogicalTypes.md
 */
object FromParquetSchema {

  fun fromParquet(type: Type): Schema {
    return when (type) {
      is PrimitiveType -> fromPrimitiveType(type)
      is MessageType -> fromGroupType(type)
      is GroupType -> fromGroupType(type)
      else -> throw UnsupportedOperationException()
    }
  }

  fun fromMessageType(messageType: MessageType): Schema.Struct {
    return fromStruct(messageType)
  }

  private fun fromGroupType(groupType: GroupType): Schema {
    return if (groupType.logicalTypeAnnotation is LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
      fromMap(groupType)
    } else fromStruct(groupType)
  }

  private fun fromStruct(groupType: GroupType): Schema.Struct {
    val fields = groupType.fields.map {
      val fieldType = fromParquet(it)
      Schema.Field(it.name, if (it.repetition.isNullable()) fieldType.nullable() else fieldType)
    }
    return Schema.Struct(groupType.name, fields)
  }

  private fun fromMap(groupType: GroupType): Schema.Map {
    val value = fromParquet(groupType.fields[0].asGroupType().fields[1])
    return Schema.Map(value)
  }

  fun fromPrimitiveType(type: PrimitiveType): Schema {

    fun int32(annotation: LogicalTypeAnnotation?): Schema = when (annotation) {
      is LogicalTypeAnnotation.IntLogicalTypeAnnotation -> when (annotation.bitWidth) {
        8 -> Schema.Int8
        16 -> Schema.Int16
        32 -> Schema.Int32
        64 -> Schema.Int64
        else -> Schema.Int32
      }
//      LogicalTypeAnnotation.DATE -> DateType
//      LogicalTypeAnnotation.TIME_MILLIS -> TimeMillisType
      else -> Schema.Int32
    }

    fun int64(annotation: LogicalTypeAnnotation?): Schema = when (annotation) {
      is LogicalTypeAnnotation.IntLogicalTypeAnnotation -> when (annotation.bitWidth) {
        8 -> Schema.Int8
        16 -> Schema.Int16
        32 -> Schema.Int32
        64 -> Schema.Int64
        else -> Schema.Int32
      }
      is LogicalTypeAnnotation.TimestampLogicalTypeAnnotation -> Schema.TimestampMillis
      else -> Schema.Int64
    }

    fun binary(type: PrimitiveType, annotation: LogicalTypeAnnotation?, length: Int): Schema = when (annotation) {
      is LogicalTypeAnnotation.EnumLogicalTypeAnnotation -> Schema.Enum(emptyList())
      is LogicalTypeAnnotation.StringLogicalTypeAnnotation -> if (length > 0) Schema.Varchar(length) else Schema.Strings
//      OriginalType.DECIMAL -> {
//        val meta = type.decimalMetadata
//        DecimalType(Precision(meta.precision), Scale(meta.scale))
//      }
      else -> Schema.Bytes
    }

    val element: Schema = when (type.primitiveTypeName) {
      PrimitiveType.PrimitiveTypeName.BINARY -> binary(type, type.logicalTypeAnnotation, type.typeLength)
      PrimitiveType.PrimitiveTypeName.BOOLEAN -> Schema.Booleans
      PrimitiveType.PrimitiveTypeName.DOUBLE -> Schema.Float64
      PrimitiveType.PrimitiveTypeName.FLOAT -> Schema.Float32
      PrimitiveType.PrimitiveTypeName.INT32 -> int32(type.logicalTypeAnnotation)
      PrimitiveType.PrimitiveTypeName.INT64 -> int64(type.logicalTypeAnnotation)
      // INT96 is deprecated, but it's commonly used (wrongly) for timestamp millis
      // Spark does this, so we must do too
      // https://issues.apache.org/jira/browse/PARQUET-323
      PrimitiveType.PrimitiveTypeName.INT96 -> Schema.TimestampMillis
      PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> binary(type, type.logicalTypeAnnotation, type.typeLength)
      null -> error("primitiveTypeName cannot be null ${type.primitiveTypeName}")
    }

    return if (type.isRepeated()) Schema.Array(element) else element
  }
}

private fun Type.Repetition.isNullable(): Boolean =
    this == Type.Repetition.OPTIONAL

private fun PrimitiveType.isRepeated(): Boolean {
  return repetition == Type.Repetition.REPEATED
}
