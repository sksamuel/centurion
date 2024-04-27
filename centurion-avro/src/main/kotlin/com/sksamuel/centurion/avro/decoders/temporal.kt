package com.sksamuel.centurion.avro.decoders

import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion
import org.apache.avro.data.TimeConversions.TimestampMillisConversion
import java.time.Instant

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
