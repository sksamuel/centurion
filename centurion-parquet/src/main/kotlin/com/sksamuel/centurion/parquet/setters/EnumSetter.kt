package com.sksamuel.centurion.parquet.setters

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer

object EnumSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) {
    when (value) {
      is String -> consumer.addBinary(Binary.fromString(value))
      else -> throw UnsupportedOperationException("Unsupported value $value for enum type")
    }
  }

}
