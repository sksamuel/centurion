package com.sksamuel.reactivehive.parquet.converters

import org.apache.parquet.io.api.PrimitiveConverter
import java.sql.Timestamp
import java.time.Instant

// see https://issues.apache.org/jira/browse/HIVE-6394 and
// https://issues.apache.org/jira/browse/SPARK-10177 for implementation ideas
class TimestampConverter(private val receiver: Receiver<Timestamp>) : PrimitiveConverter() {

  // The Julian Date (JD) is the number of days (with decimal fraction of the day) that
  // have elapsed since 12 noon UTC on the Julian epoch
//  private val nanosInDay = 24L * 60L * 60 * 1000 * 1000 * 1000

  override fun addLong(value: Long) {
    receiver.add(Timestamp.from(Instant.ofEpochMilli(value)))
  }

//  override fun addBinary(x: Binary) {
//    // first 8 bytes are the nanoseconds since noon
//    // second 4 bytes are the days since the JD epoch
//    val nano = NanoTime.fromBinary(x)
//
//    // the first arg is the number of days since Monday, January 1, 4713 BC
//    // the second arg is the decimal fraction between 0 and 1 of the number of elapsed nanos, with
//    // 0.0 being midnight and 1.0 being 1 nanosecond before the next day
//    val julianDate = JulianDate(nano.julianDay, nano.timeOfDayNanos.toDouble() / nanosInDay)
//    builder[field.name] = Timestamp.from(julianDate.toLocalDateTime().toInstant(ZoneOffset.UTC))
//  }
}