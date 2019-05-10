package com.sksamuel.reactivehive.parquet

import com.sksamuel.reactivehive.ArrayType
import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.ByteType
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.DecimalType
import com.sksamuel.reactivehive.DoubleType
import com.sksamuel.reactivehive.EnumType
import com.sksamuel.reactivehive.FloatType
import com.sksamuel.reactivehive.IntType
import com.sksamuel.reactivehive.LongType
import com.sksamuel.reactivehive.MapDataType
import com.sksamuel.reactivehive.ShortType
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TimeMicrosType
import com.sksamuel.reactivehive.TimeMillisType
import com.sksamuel.reactivehive.TimestampMicrosType
import com.sksamuel.reactivehive.TimestampMillisType
import com.sksamuel.reactivehive.Type
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types

/**
 * Conversion functions to parquet types from reactive-hive types.
 *
 * Parquet types are defined at the parquet repo:
 * https://github.com/apache/parquet-format/blob/c6d306daad4910d21927b8b4447dc6e9fae6c714/LogicalTypes.md
 */
object ToParquetSchema {

  fun toMessageType(struct: StructType, name: String = "root"): MessageType {
    val types = struct.fields.map {
      toParquetType(it.type,
          it.name,
          it.nullable)
    }
    return MessageType(name, *types.toTypedArray())
  }

  fun toParquetType(type: Type, name: String, nullable: Boolean): org.apache.parquet.schema.Type {
    val repetition = if (nullable) Repetition.OPTIONAL else Repetition.REQUIRED
    return when (type) {
      is StructType -> {
        val fields = type.fields.map {
          toParquetType(it.type,
              it.name,
              it.nullable)
        }
        Types.buildGroup(repetition).addFields(*fields.toTypedArray()).named(name)
      }
      // note: a Parquet string field, with a defined length, cannot be read in hive, so must not set a length here
      StringType -> Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).`as`(OriginalType.UTF8).named(
          name)
      is BooleanType -> Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(name)
      BinaryType -> Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(name)
      DoubleType -> Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(name)
      FloatType -> Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(name)
      ByteType -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
          .`as`(OriginalType.INT_8).named(name)
      IntType -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(name)
      LongType -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(name)
      ShortType -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
          .`as`(OriginalType.INT_16).named(name)
      TimestampMillisType -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
          .`as`(OriginalType.TIMESTAMP_MILLIS).named(name)
      TimestampMicrosType -> TODO()
      TimeMicrosType -> TODO()
      TimeMillisType -> TODO()
      DateType -> TODO()
      is MapDataType -> {
        val key = toParquetType(type.keyType, "key", false)
        val value = toParquetType(type.valueType, "value", true)
        Types.map(repetition).key(key).value(value).named(name)
      }
      // https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md#decimal
      is DecimalType -> TODO()
      is EnumType -> Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
          .`as`(OriginalType.ENUM).named(name)
      // in parquet, the elements of a list must be called "element", and they cannot be null
      // the nullability of list elements is handled in the containing type, represented here by repetition
      is ArrayType -> Types.list(repetition).element(toParquetType(
          type.elementType,
          "element",
          false)).named(name)
    }
  }
}