package com.sksamuel.centurion.parquet.writers

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import org.apache.parquet.io.api.RecordConsumer
import java.math.RoundingMode

internal class StructWriter(
  private val schema: Schema.Struct,
  roundingMode: RoundingMode,
  // set to true when the initial root message, otherwise false for GroupType
  private val root: Boolean
) : Writer {

  override fun write(consumer: RecordConsumer, value: Any) {

    fun write() {

      val values = when (value) {
        is Struct -> value.values
        else -> throw UnsupportedOperationException("$value is not recognized as a struct type")
      }

      schema.fields.forEachIndexed { k, field ->
        val fieldValue = values[k]
        // null values are handled in parquet by skipping them completely in the file
        if (fieldValue != null) {
          consumer.writeField(field.name, k) {
            val writer = Writer.writerFor(field.schema)
            writer.write(consumer, fieldValue)
          }
        }
      }
    }

    if (root) {
      consumer.writeMessage { write() }
    } else {
      consumer.writeGroup { write() }
    }
  }
}
