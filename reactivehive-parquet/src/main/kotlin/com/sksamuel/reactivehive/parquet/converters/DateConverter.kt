package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.StructField
import org.apache.parquet.io.api.PrimitiveConverter
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*

class DateConverter(private val field: StructField,
                    private val builder: MutableMap<String, Any?>) : PrimitiveConverter() {

  private val UnixEpoch = LocalDateTime.of(1970, 1, 1, 0, 0, 0)

  override fun addInt(value: Int) {
    val dt = UnixEpoch.plusDays(value.toLong())
    val millis = dt.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli()
    builder[field.name] = Date(millis)
  }
}