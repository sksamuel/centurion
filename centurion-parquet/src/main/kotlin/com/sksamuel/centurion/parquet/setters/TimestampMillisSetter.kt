package com.sksamuel.centurion.parquet.setters

import org.apache.parquet.io.api.RecordConsumer
import java.sql.Timestamp

/**
 * TIMESTAMP_MILLIS is used for a combined logical date and time type, with millisecond precision.
 * It must annotate an int64 that stores the number of milliseconds from the Unix epoch,
 * 00:00:00.000 on 1 January 1970, UTC.
 *
 * The sort order used for TIMESTAMP\_MILLIS is signed.
 */
object TimestampMillisSetter : Setter {

  private const val nanosInDay = 24L * 60L * 60 * 1000 * 1000 * 1000

  // first 8 bytes are the nanoseconds
  // second 4 bytes are the days
  override fun set(consumer: RecordConsumer, value: Any) {
    val ts = value as Timestamp
    consumer.addLong(ts.toInstant().toEpochMilli())
  }
}
