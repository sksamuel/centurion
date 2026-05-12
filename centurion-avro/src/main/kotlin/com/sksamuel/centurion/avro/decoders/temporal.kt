package com.sksamuel.centurion.avro.decoders

import org.apache.avro.LogicalTypes
import org.apache.avro.LogicalTypes.LocalTimestampMicros
import org.apache.avro.LogicalTypes.LocalTimestampMillis
import org.apache.avro.LogicalTypes.TimeMicros
import org.apache.avro.LogicalTypes.TimeMillis
import org.apache.avro.Schema
import org.apache.avro.data.TimeConversions.LocalTimestampMicrosConversion
import org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion
import org.apache.avro.data.TimeConversions.TimeMicrosConversion
import org.apache.avro.data.TimeConversions.TimeMillisConversion
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion
import org.apache.avro.data.TimeConversions.TimestampMillisConversion
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit

private val LOCAL_TIMESTAMP_MILLIS_CONVERSION = LocalTimestampMillisConversion()
private val LOCAL_TIMESTAMP_MICROS_CONVERSION = LocalTimestampMicrosConversion()
private val TIME_MILLIS_CONVERSION = TimeMillisConversion()
private val TIME_MICROS_CONVERSION = TimeMicrosConversion()
private val TIMESTAMP_MILLIS_CONVERSION = TimestampMillisConversion()
private val TIMESTAMP_MICROS_CONVERSION = TimestampMicrosConversion()

/**
 * [Decoder] for [Instant] which supports [LogicalTypes.TimestampMillis], [LogicalTypes.TimestampMicros] and Longs.
 */
object InstantDecoder : Decoder<Instant> {
   override fun decode(schema: Schema, value: Any?): Instant {
      return when (value) {
         is Long -> {
            when (val logicalType = schema.logicalType) {
               is LogicalTypes.TimestampMillis -> TIMESTAMP_MILLIS_CONVERSION.fromLong(value, schema, logicalType)
               is LogicalTypes.TimestampMicros -> TIMESTAMP_MICROS_CONVERSION.fromLong(value, schema, logicalType)
               null -> Instant.ofEpochMilli(value)
               else -> error("Unsupported schema for Instant: $schema")
            }
         }

         else -> error("Unsupported schema and value for Instant: $schema $value")
      }
   }
}

/**
 * [Decoder] for [LocalTime] which supports [TimeMillis], [TimeMicros] and Longs.
 */
object LocalTimeDecoder : Decoder<LocalTime> {
   override fun decode(schema: Schema, value: Any?): LocalTime {
      return when (value) {
         is Long -> {
            when (val logicalType = schema.logicalType) {
               is TimeMicros -> TIME_MICROS_CONVERSION.fromLong(value, schema, logicalType)
               null -> LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value))
               else -> error("Unsupported schema for LocalTime: $schema")
            }
         }

         is Int -> {
            when (val logicalType = schema.logicalType) {
               is TimeMillis -> TIME_MILLIS_CONVERSION.fromInt(value, schema, logicalType)
               else -> error("Unsupported schema for LocalTime: $schema")
            }
         }

         else -> error("Unsupported schema and value for LocalTime: $schema $value")
      }
   }
}

object OffsetDateTimeDecoder : Decoder<OffsetDateTime> {
   override fun decode(schema: Schema, value: Any?): OffsetDateTime =
      InstantDecoder.decode(schema, value).atOffset(ZoneOffset.UTC)
}

/**
 * [Decoder] for [LocalDateTime] which supports [LocalTimestampMillis], [LocalTimestampMicros] and Longs.
 */
object LocalDateTimeDecoder : Decoder<LocalDateTime> {
   override fun decode(schema: Schema, value: Any?): LocalDateTime {
      return when (value) {
         is Long -> {
            when (val logicalType = schema.logicalType) {
               is LocalTimestampMillis -> LOCAL_TIMESTAMP_MILLIS_CONVERSION.fromLong(value, schema, logicalType)
               is LocalTimestampMicros -> LOCAL_TIMESTAMP_MICROS_CONVERSION.fromLong(value, schema, logicalType)
               null -> LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC)
               else -> error("Unsupported schema for LocalDateTime: $schema")
            }
         }

         else -> error("Unsupported schema and value for LocalDateTime: $schema $value")
      }
   }
}
