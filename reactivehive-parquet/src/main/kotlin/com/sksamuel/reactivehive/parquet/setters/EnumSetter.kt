package com.sksamuel.reactivehive.parquet.setters

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer

object EnumSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) {
    when (value) {
      is String -> consumer.addBinary(Binary.fromString(value))
      else -> throw UnsupportedOperationException("Unsupported value $value for enum type")
    }
  }

}
