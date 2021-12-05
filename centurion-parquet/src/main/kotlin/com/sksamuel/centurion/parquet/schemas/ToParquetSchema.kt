package com.sksamuel.centurion.parquet.schemas

import com.sksamuel.centurion.Schema
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types

/**
 * Conversion functions to parquet types from centurion types.
 *
 * Parquet types are defined at the parquet repo:
 * https://github.com/apache/parquet-format/blob/c6d306daad4910d21927b8b4447dc6e9fae6c714/LogicalTypes.md
 */
object ToParquetSchema {

  /**
   * Returns a parquet [MessageType] for the given centurion [Record] schema.
   */
  fun toMessageType(schema: Schema.Struct): MessageType {
    val types = schema.fields.map { toParquetType(it.schema, it.name) }
    val builder: Types.GroupBuilder<MessageType> = Types.buildMessage()
    return types.fold(builder) { acc, op -> acc.addField(op) }.named(schema.name)
  }

  // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
  fun toParquetType(schema: Schema, name: String): Type {

    val extracted = if (schema is Schema.Nullable) schema.element else schema
    val repetition = if (schema is Schema.Nullable) Repetition.OPTIONAL else Repetition.REQUIRED

    return when (extracted) {

      is Schema.Struct -> {
        val fields = extracted.fields.map { toParquetType(it.schema, it.name) }
        Types.buildGroup(repetition).addFields(*fields.toTypedArray()).named(name)
      }

      // for parquet arrays we should have a container group, which is a logical type list
      // and has the repetition of the list field itself.
      // this group then has a repeated field of type group called 'list'.
      // this repeated group has a single field called 'element' which is the type of the input data elements
      //
      //   <field-repetition> group <field-name> (LIST) {
      //     repeated group list {
      //       <list-element-repetition> <list-element-type> element;
      //     }
      //   }
      //
      // note that the names 'list' and 'element' are hardcoded values.
      is Schema.Array -> {
        Types.list(repetition).element(toParquetType(extracted.elements, "element")).named(name)
      }

      /**
       * STRING may only be used to annotate the binary primitive type and indicates that the byte array should be interpreted as a UTF-8 encoded character string.
       * The sort order used for STRING strings is unsigned byte-wise comparison.
       * Compatibility
       * STRING corresponds to UTF8 ConvertedType.
       *
       * Note: a Parquet string field, with a defined length, cannot be read in hive, so must not set a length here
       */
      Schema.Strings ->
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
          .`as`(LogicalTypeAnnotation.stringType()).named(name)

      Schema.Booleans -> Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition).named(name)
      Schema.Bytes -> Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition).named(name)
      Schema.Float64 -> Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition).named(name)
      Schema.Float32 -> Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition).named(name)
      Schema.Int8 -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
        .`as`(OriginalType.INT_8).named(name)
      Schema.Int32 -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition).named(name)
      Schema.Int64 -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition).named(name)
      Schema.Int16 -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
        .`as`(OriginalType.INT_16).named(name)
      Schema.TimestampMillis -> Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
        .`as`(OriginalType.TIMESTAMP_MILLIS).named(name)
//      Schema.TimestampMicros -> TODO()

      /**
       * TIME is used for a logical time type without a date with millisecond or microsecond precision.
       * The type has two type parameters: UTC adjustment (true or false) and precision (MILLIS or MICROS, NANOS).
       * TIME with precision MICROS is used for microsecond precision. It must annotate an int64 that stores the number of microseconds after midnight.
       * The sort order used for TIME is signed.
       */
//      Schema.TimeMicrosType ->
//        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
//            .`as`(OriginalType.TIME_MICROS).named(name)

      /**
       * TIME is used for a logical time type without a date with millisecond or microsecond precision.
       * The type has two type parameters: UTC adjustment (true or false) and precision (MILLIS or MICROS, NANOS).
       * TIME with precision MILLIS is used for millisecond precision. It must annotate an int32 that stores the number of milliseconds after midnight.
       * The sort order used for TIME is signed.
       */
//      Schema.TimeMillisType ->
//        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
//            .`as`(OriginalType.TIME_MILLIS).named(name)

      /**
       * DATE is used to for a logical date type, without a time of day.
       * It must annotate an int32 that stores the number of days from the Unix epoch, 1 January 1970.
       * The sort order used for DATE is signed.
       */
//      Schema.DateType ->
//        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
//            .`as`(OriginalType.DATE).named(name)

      is Schema.Map -> {
        val key = Types
          .required(PrimitiveType.PrimitiveTypeName.BINARY)
          .`as`(LogicalTypeAnnotation.stringType())
          .named("key")
        val value = toParquetType(extracted.values, "value")
        Types.map(repetition).key(key).value(value).named(name)
      }

      /**
       * ENUM annotates the binary primitive type and indicates that the value
       * was converted from an enumerated type in another data model (e.g. Thrift, Avro, Protobuf).
       * Applications using a data model lacking a native enum type should interpret
       * ENUM annotated field as a UTF-8 encoded string.
       * The sort order used for ENUM values is unsigned byte-wise comparison.
       */
      is Schema.Enum ->
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
          .`as`(LogicalTypeAnnotation.enumType()).named(name)

      // DECIMAL can be used to annotate the following types:
      // int32: for 1 <= precision <= 9
      // int64: for 1 <= precision <= 18; precision < 10 will produce a warning
      // fixed_len_byte_array: precision is limited by the array size. Length n can store <= floor(log_10(2^(8*n - 1) - 1)) base-10 digits
      // binary: precision is not limited, but is required. The minimum number of bytes to store the unscaled value should be used.
      // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
      is Schema.Decimal -> when {
        extracted.precision.value <= 9 ->
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
            .`as`(LogicalTypeAnnotation.decimalType(extracted.scale.value, extracted.precision.value))
            .named(name)
        extracted.precision.value <= 18 ->
          Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
            .`as`(LogicalTypeAnnotation.decimalType(extracted.scale.value, extracted.precision.value))
            .named(name)
        else -> error("Unsupported precision ${extracted.precision}")
      }

      Schema.Nulls -> TODO()
      is Schema.Varchar -> Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
        .`as`(LogicalTypeAnnotation.stringType()).named(name)
      is Schema.Nullable -> error("Should be extracted")
    }
  }
}
