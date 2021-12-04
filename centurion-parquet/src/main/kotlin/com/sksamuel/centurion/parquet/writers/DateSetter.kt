package com.sksamuel.centurion.parquet.writers

import org.apache.parquet.io.api.RecordConsumer
import java.sql.Date
import java.time.LocalDate

object DateSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) {
    when (value) {
      is Date -> consumer.addInteger(value.toLocalDate().toEpochDay().toInt())
      is LocalDate -> consumer.addInteger(value.toEpochDay().toInt())
      else -> throw UnsupportedOperationException()
    }
  }

}
