package com.sksamuel.rxhive.parquet.converters

import com.sksamuel.rxhive.TimeMillis
import org.apache.parquet.io.api.PrimitiveConverter

class TimeMillisConverter(private val receiver: Receiver<Any>) : PrimitiveConverter() {
  override fun addInt(value: Int) {
    receiver.add(TimeMillis(value))
  }
}
