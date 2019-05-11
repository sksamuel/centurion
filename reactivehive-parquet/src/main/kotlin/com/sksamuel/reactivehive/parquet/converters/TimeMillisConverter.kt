package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.TimeMillis
import org.apache.parquet.io.api.PrimitiveConverter

class TimeMillisConverter(private val receiver: Receiver<Any>) : PrimitiveConverter() {
  override fun addInt(value: Int) {
    receiver.add(TimeMillis(value))
  }
}
