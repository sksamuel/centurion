package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.StructField
import jodd.time.JulianDate
import org.apache.parquet.example.data.simple.NanoTime
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter
import java.sql.Timestamp
import java.time.ZoneOffset

// see https://issues.apache.org/jira/browse/HIVE-6394 and
// https://issues.apache.org/jira/browse/SPARK-10177 for implementation ideas
class TimestampPrimitiveConverter(private val field: StructField,
                                  private val builder: MutableMap<String, Any?>) : PrimitiveConverter() {

  // The Julian Date (JD) is the number of days (with decimal fraction of the day) that
  // have elapsed since 12 noon UTC on the Julian epoch
  private val nanosOfDay = 24L * 60L * 60 * 1000 * 1000 * 1000

  override fun addBinary(x: Binary) {
    // first 8 bytes are the nanoseconds since noon
    // second 4 bytes are the days since the JD epoch
    val nano = NanoTime.fromBinary(x)

    // the first arg is the number of days since Monday, January 1, 4713 BC
    // the second arg is the decimal fraction between 0 and 1 of the number of elapsed nanos, with
    // 0 being midnight and 1 being 1 nanosecond before the next midnight
    val julianDate = JulianDate(nano.julianDay, nano.timeOfDayNanos.toDouble() / nanosOfDay)
    builder[field.name] = Timestamp.from(julianDate.toLocalDateTime().toInstant(ZoneOffset.UTC))
  }
}