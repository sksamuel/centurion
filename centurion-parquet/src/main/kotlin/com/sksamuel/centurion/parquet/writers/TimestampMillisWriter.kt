package com.sksamuel.centurion.parquet.writers

import org.apache.parquet.io.api.RecordConsumer
import java.sql.Timestamp
import java.time.Instant

/**
 * TIMESTAMP_MILLIS is used for a combined logical date and time type, with millisecond precision.
 * It must annotate an int64 that stores the number of milliseconds from the Unix epoch,
 * 00:00:00.000 on 1 January 1970, UTC.
 *
 * The sort order used for TIMESTAMP_MILLIS is signed.
 */
object TimestampMillisWriter : Writer {

  private const val nanosInDay = 24L * 60L * 60 * 1000 * 1000 * 1000

  // first 8 bytes are the nanoseconds
  // second 4 bytes are the days
  override fun write(consumer: RecordConsumer, value: Any) {
    val millis = when (value) {
      is Timestamp -> value.toInstant().toEpochMilli()
      is Instant -> value.toEpochMilli()
      is Long -> value
      else -> error("Unsupported timestamp type $value")
    }
    consumer.addLong(millis)
  }
}
