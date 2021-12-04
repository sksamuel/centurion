package com.sksamuel.rxhive.parquet.converters

import org.apache.parquet.io.api.PrimitiveConverter
import java.time.LocalDate

class DateConverter(private val receiver: Receiver<LocalDate>) : PrimitiveConverter() {

  override fun addInt(value: Int) {
    val date = LocalDate.ofEpochDay(value.toLong())
    receiver.add(date)
  }
}