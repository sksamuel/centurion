package com.sksamuel.reactivehive.parquet.converters

import jodd.time.JulianDate
import org.apache.parquet.example.data.simple.NanoTime
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter
import java.sql.Timestamp
import java.time.Instant
import java.time.ZoneOffset

// see https://issues.apache.org/jira/browse/HIVE-6394 and
// https://issues.apache.org/jira/browse/SPARK-10177
class TimestampConverter(private val receiver: Receiver<Timestamp>) : PrimitiveConverter() {

  // The Julian Date (JD) is the number of days (with decimal fraction of the day) that
  // have elapsed since 12 noon UTC on the Julian epoch
  private val nanosInDay = 24L * 60L * 60 * 1000 * 1000 * 1000

  override fun addLong(value: Long) {
    receiver.add(Timestamp.from(Instant.ofEpochMilli(value)))
  }

  /**
   * Since INT96 is sometimes used as timestamps too
   * See https://issues.apache.org/jira/browse/PARQUET-323
   * https://issues.apache.org/jira/browse/PARQUET-861
   * https://stackoverflow.com/questions/42628287/sparks-int96-time-type
   *
   *  Timestamps saved as an `int96` are made up of the nanoseconds in the day
   *  (first 8 byte) and the Julian day (last 4 bytes). No timezone is attached to this value.
   *  To convert the timestamp into nanoseconds since the Unix epoch, 00:00:00.000000
   *  on 1 January 1970, the following formula can be used:
   *  `(julian_day - 2440588) * (86400 * 1000 * 1000 * 1000) + nanoseconds`.
   *  The magic number `2440588` is the julian day for 1 January 1970.
   *
   * Note that these timestamps are the common usage of the `int96` physical type and are not
   * marked with a special logical type annotation.
   */
  override fun addBinary(value: Binary) {
    // the first arg is the number of days since Monday, January 1, 4713 BC
    // the second arg is the decimal fraction between 0 and 1 of the number of elapsed nanos, with
    // 0.0 being midnight and 1.0 being 1 nanosecond before the next day
    val nano = NanoTime.fromBinary(value)
    val julianDate = JulianDate(nano.julianDay, nano.timeOfDayNanos.toDouble() / nanosInDay)
    val ts = Timestamp.from(julianDate.toLocalDateTime().toInstant(ZoneOffset.UTC))
    //   val nanos = (nano.julianDay.toLong() - 2440588L) * (86400L * 1000 * 1000 * 1000) + nano.timeOfDayNanos
//    val millis = nanos / 1000 / 1000
//    val ts = Timestamp.from(Instant.ofEpochMilli(millis))
    receiver.add(ts)
  }

//  override fun addBinary(x: Binary) {
//
//
//    // the first arg is the number of days since Monday, January 1, 4713 BC
//    // the second arg is the decimal fraction between 0 and 1 of the number of elapsed nanos, with
//    // 0.0 being midnight and 1.0 being 1 nanosecond before the next day
//    val julianDate = JulianDate(nano.julianDay, nano.timeOfDayNanos.toDouble() / nanosInDay)
//    builder[field.name] = Timestamp.from(julianDate.toLocalDateTime().toInstant(ZoneOffset.UTC))
//  }
}