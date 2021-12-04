package com.sksamuel.centurion.parquet.writers

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import org.apache.parquet.io.api.RecordConsumer
import java.math.RoundingMode

class StructWriter(
  private val schema: Schema.Struct,
  roundingMode: RoundingMode,
  // set to true when the initial root message, otherwise false for GroupType
  private val root: Boolean
) : Writer {

  override fun write(consumer: RecordConsumer, value: Any) {

    // in parquet, nested types must be wrapped in "groups"
    fun writeGroup(fn: () -> Unit) {
      consumer.startGroup()
      fn()
      consumer.endGroup()
    }

    // top level types must be wrapped in messages
    fun writeMessage(fn: () -> Unit) {
      consumer.startMessage()
      fn()
      consumer.endMessage()
    }

    fun writeField(name: String, index: Int, fn: () -> Unit) {
      consumer.startField(name, index)
      fn()
      consumer.endField(name, index)
    }

    fun write() {

      val values = when (value) {
        is Struct -> value.values
        else -> throw UnsupportedOperationException("$value is not recognized as a struct type")
      }

      schema.fields.forEachIndexed { k, field ->
        val fieldValue = values[k]
        // null values are handled in parquet by skipping them completely in the file
        if (fieldValue != null) {
          writeField(field.name, k) {
            val writer = Writer.writerFor(field.schema)
            writer.write(consumer, fieldValue)
          }
        }
      }
    }

    if (root) {
      writeMessage { write() }
    } else {
      writeGroup { write() }
    }
  }
}
