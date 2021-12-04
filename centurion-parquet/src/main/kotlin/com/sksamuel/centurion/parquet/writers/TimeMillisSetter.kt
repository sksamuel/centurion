package com.sksamuel.centurion.parquet.writers

import com.sksamuel.centurion.TimeMillis
import org.apache.parquet.io.api.RecordConsumer

object TimeMillisSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) {
    val millis = when (value) {
      is TimeMillis -> value.millis
      is Int -> value
      else -> throw UnsupportedOperationException()
    }
    consumer.addInteger(millis)
  }
}
