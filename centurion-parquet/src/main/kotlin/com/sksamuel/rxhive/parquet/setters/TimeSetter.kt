package com.sksamuel.rxhive.parquet.setters

import jodd.time.JulianDate
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.sql.Timestamp

object TimeSetter : Setter {

  private const val nanosInDay = 24L * 60L * 60 * 1000 * 1000 * 1000

  // first 8 bytes are the nanoseconds
  // second 4 bytes are the days
  override fun set(consumer: RecordConsumer, value: Any) {
    val ts = value as Timestamp
    val julian = JulianDate.of(ts.toLocalDateTime())
    val bytes = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
        .putLong((julian.fraction * nanosInDay).toLong())
        .putInt(julian.julianDayNumber)
    val binary = Binary.fromReusedByteArray(bytes.array())
    consumer.addBinary(binary)
  }
}