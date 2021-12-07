package com.sksamuel.centurion.parquet.writers

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer

object EnumWriter : Writer {

  override fun write(consumer: RecordConsumer, value: Any) {
    when (value) {
      is String -> consumer.addBinary(Binary.fromString(value))
      else -> throw UnsupportedOperationException("Unsupported value $value for enum type")
    }
  }

}
