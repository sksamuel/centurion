package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.TimeMillis
import org.apache.parquet.io.api.PrimitiveConverter

class TimeMillisConverter(private val receiver: Receiver<Any>) : PrimitiveConverter() {
  override fun addInt(value: Int) {
    receiver.add(TimeMillis(value))
  }
}
