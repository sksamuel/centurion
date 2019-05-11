package com.sksamuel.reactivehive.parquet.setters

import org.apache.parquet.io.api.RecordConsumer
import java.sql.Date
import java.time.LocalDate

object DateSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) {
    when (value) {
      is Date -> consumer.addInteger(value.toLocalDate().toEpochDay().toInt())
      is LocalDate -> consumer.addInteger(value.toEpochDay().toInt())
      else -> throw UnsupportedOperationException()
    }
  }

}
