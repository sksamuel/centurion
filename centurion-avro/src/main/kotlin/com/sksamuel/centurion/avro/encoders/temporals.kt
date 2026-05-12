package com.sksamuel.centurion.avro.encoders

import org.apache.avro.LogicalTypes.LocalTimestampMicros
import org.apache.avro.LogicalTypes.LocalTimestampMillis
import org.apache.avro.LogicalTypes.TimeMicros
import org.apache.avro.LogicalTypes.TimeMillis
import org.apache.avro.LogicalTypes.TimestampMicros
import org.apache.avro.LogicalTypes.TimestampMillis
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

object LocalDateTimeEncoder : Encoder<LocalDateTime> {
   override fun encode(schema: Schema, value: LocalDateTime): Any? {
      val logicalType = schema.logicalType
      return when {
         logicalType is LocalTimestampMillis ->
            LOCAL_TIMESTAMP_MILLIS_CONVERSION.toLong(value, schema, logicalType)

         logicalType is LocalTimestampMicros ->
            LOCAL_TIMESTAMP_MICROS_CONVERSION.toLong(value, schema, logicalType)

         schema.type == Schema.Type.LONG ->
            value.toInstant(ZoneOffset.UTC).toEpochMilli()

         else -> error("Unsupported schema for LocalDateTime: $schema")
      }
   }
}

object LocalTimeEncoder : Encoder<LocalTime> {
   override fun encode(schema: Schema, value: LocalTime): Any? {
      val logicalType = schema.logicalType
      return when {
         logicalType is TimeMillis ->
            TIME_MILLIS_CONVERSION.toInt(value, schema, logicalType)

         logicalType is TimeMicros ->
            TIME_MICROS_CONVERSION.toLong(value, schema, logicalType)

         schema.type == Schema.Type.INT ->
            TimeUnit.NANOSECONDS.toMillis(value.toNanoOfDay())

         else -> error("Unsupported schema for LocalDateTime: $schema")
      }
   }
}

/**
 * An [Encoder] for [Instant].
 */
object InstantEncoder : Encoder<Instant> {
   override fun encode(schema: Schema, value: Instant): Any? {
      val logicalType = schema.logicalType
      return when {
         logicalType is TimestampMillis ->
            TIMESTAMP_MILLIS_CONVERSION.toLong(value, schema, logicalType)

         logicalType is TimestampMicros ->
            TIMESTAMP_MICROS_CONVERSION.toLong(value, schema, logicalType)

         schema.type == Schema.Type.LONG -> value.toEpochMilli()
         else -> error("Unsupported schema for LocalDateTime: $schema")
      }
   }
}

val OffsetDateTimeEncoder: Encoder<OffsetDateTime> = InstantEncoder.contraMap { it.toInstant() }
