package com.sksamuel.rxhive.parquet

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
import com.sksamuel.rxhive.StringType
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TimeMicrosType
import com.sksamuel.rxhive.TimeMillisType
import com.sksamuel.rxhive.TimestampMicrosType
import com.sksamuel.rxhive.TimestampMillisType
import com.sksamuel.rxhive.Type
import com.sksamuel.rxhive.VarcharType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types

/**
 * Conversion functions to parquet types from rxhive types.
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

  // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
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
      /**
       * STRING may only be used to annotate the binary primitive type and indicates that the byte array should be interpreted as a UTF-8 encoded character string.
       * The sort order used for STRING strings is unsigned byte-wise comparison.
       * Compatibility
       * STRING corresponds to UTF8 ConvertedType.
       *
       * Note: a Parquet string field, with a defined length, cannot be read in hive, so must not set a length here
       */
      StringType ->
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
          .`as`(OriginalType.UTF8).named(name)

      is BooleanType -> Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(name)
      BinaryType -> Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(name)
      Float64Type -> Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(name)
      Float32Type -> Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(name)
      Int8Type -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
          .`as`(OriginalType.INT_8).named(name)
      Int32Type -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(name)
      Int64Type -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(name)
      Int16Type -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
          .`as`(OriginalType.INT_16).named(name)
      TimestampMillisType -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
          .`as`(OriginalType.TIMESTAMP_MILLIS).named(name)
      TimestampMicrosType -> TODO()

      /**
       * TIME is used for a logical time type without a date with millisecond or microsecond precision.
       * The type has two type parameters: UTC adjustment (true or false) and precision (MILLIS or MICROS, NANOS).
       * TIME with precision MICROS is used for microsecond precision. It must annotate an int64 that stores the number of microseconds after midnight.
       * The sort order used for TIME is signed.
       */
      TimeMicrosType ->
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
            .`as`(OriginalType.TIME_MICROS).named(name)

      /**
       * TIME is used for a logical time type without a date with millisecond or microsecond precision.
       * The type has two type parameters: UTC adjustment (true or false) and precision (MILLIS or MICROS, NANOS).
       * TIME with precision MILLIS is used for millisecond precision. It must annotate an int32 that stores the number of milliseconds after midnight.
       * The sort order used for TIME is signed.
       */
      TimeMillisType ->
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .`as`(OriginalType.TIME_MILLIS).named(name)

      /**
       * DATE is used to for a logical date type, without a time of day.
       * It must annotate an int32 that stores the number of days from the Unix epoch, 1 January 1970.
       * The sort order used for DATE is signed.
       */
      DateType ->
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .`as`(OriginalType.DATE).named(name)

      is MapDataType -> {
        val key = toParquetType(type.keyType, "key", false)
        val value = toParquetType(type.valueType, "value", true)
        Types.map(repetition).key(key).value(value).named(name)
      }

      is DecimalType -> TODO()

      /**
       * ENUM annotates the binary primitive type and indicates that the value
       * was converted from an enumerated type in another data model (e.g. Thrift, Avro, Protobuf).
       * Applications using a data model lacking a native enum type should interpret
       * ENUM annotated field as a UTF-8 encoded string.
       * The sort order used for ENUM values is unsigned byte-wise comparison.
       */
      is EnumType ->
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
          .`as`(OriginalType.ENUM).named(name)
      // in parquet, the elements of a list must be called "element", and they cannot be null
      // the nullability of list elements is handled in the containing type, represented here by repetition
      is ArrayType -> Types.list(repetition).element(toParquetType(
          type.elementType,
          "element",
          false)).named(name)
      is CharType -> TODO()
      is VarcharType -> TODO()
      BigIntType -> TODO()
    }
  }
}