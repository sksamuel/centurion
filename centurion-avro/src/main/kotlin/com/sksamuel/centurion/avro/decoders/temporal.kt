package com.sksamuel.centurion.avro.decoders

import org.apache.avro.LogicalTypes
import org.apache.avro.LogicalTypes.TimeMicros
import org.apache.avro.LogicalTypes.TimeMillis
import org.apache.avro.Schema
import org.apache.avro.data.TimeConversions.TimeMicrosConversion
import org.apache.avro.data.TimeConversions.TimeMillisConversion
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion
import org.apache.avro.data.TimeConversions.TimestampMillisConversion
import java.time.Instant
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit

/**
 * [Decoder] for [Instant] which supports [LogicalTypes.TimestampMillis], [LogicalTypes.TimestampMicros] and Longs.
 */
object InstantDecoder : Decoder<Instant> {
   override fun decode(schema: Schema, value: Any?): Instant {
      return when (value) {
         is Long -> {
            when (val logicalType = schema.logicalType) {
               is LogicalTypes.TimestampMillis -> TimestampMillisConversion().fromLong(value, schema, logicalType)
               is LogicalTypes.TimestampMicros -> TimestampMicrosConversion().fromLong(value, schema, logicalType)
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
               is TimeMicros -> TimeMicrosConversion().fromLong(value, schema, logicalType)
               null -> LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(value))
               else -> error("Unsupported schema for Instant: $schema")
            }
         }

         is Int -> {
            when (val logicalType = schema.logicalType) {
               is TimeMillis -> TimeMillisConversion().fromInt(value, schema, logicalType)
               else -> error("Unsupported schema for Instant: $schema")
            }
         }

         else -> error("Unsupported schema and value for Instant: $schema $value")
      }
   }
}

val OffsetDateTimeDecoder: Decoder<OffsetDateTime> = InstantDecoder.map { it.atOffset(ZoneOffset.UTC) }
