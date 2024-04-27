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
import java.time.ZoneOffset

object LocalDateTimeEncoder : Encoder<LocalDateTime> {
   override fun encode(schema: Schema, value: LocalDateTime): Any? {
      return when {
         schema.logicalType is LocalTimestampMillis -> LocalTimestampMillisConversion().toLong(value, schema, schema.logicalType)
         schema.logicalType is LocalTimestampMicros -> LocalTimestampMicrosConversion().toLong(value, schema, schema.logicalType)
         schema.type == Schema.Type.LONG -> value.toInstant(ZoneOffset.UTC).toEpochMilli()
         else -> error("Unsupported schema for LocalDateTime: $schema")
      }
   }
}

object LocalTimeEncoder : Encoder<LocalTime> {
   override fun encode(schema: Schema, value: LocalTime): Any? {
      return when {
         schema.logicalType is TimeMillis -> TimeMillisConversion().toInt(value, schema, schema.logicalType)
         schema.logicalType is TimeMicros -> TimeMicrosConversion().toLong(value, schema, schema.logicalType)
         schema.type == Schema.Type.LONG -> value.toNanoOfDay()
         else -> error("Unsupported schema for LocalDateTime: $schema")
      }
   }
}

/**
 * An [Encoder] for [Instant].
 */
object InstantEncoder : Encoder<Instant> {
   override fun encode(schema: Schema, value: Instant): Long {
      return when {
         schema.logicalType is TimestampMillis -> TimestampMillisConversion().toLong(value, schema, schema.logicalType)
         schema.logicalType is TimestampMicros -> TimestampMicrosConversion().toLong(value, schema, schema.logicalType)
         schema.type == Schema.Type.LONG -> value.toEpochMilli()
         else -> error("Unsupported schema for LocalDateTime: $schema")
      }
   }
}
