package com.sksamuel.centurion.parquet.setters

import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructType
import org.apache.parquet.io.api.RecordConsumer
import java.math.RoundingMode

class StructSetter(private val type: StructType,
                   roundingMode: RoundingMode,
    // set to true when the initial root message, otherwise false for GroupType
                   private val root: Boolean) : Setter {

  override fun set(consumer: RecordConsumer, value: Any) {

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

      type.fields.forEachIndexed { k, field ->
        val fieldValue = values[k]
        // null values are handled in parquet by skipping them completely in the file
        if (fieldValue != null) {
          writeField(field.name, k) {
            val setter = Setter.writerFor(field.type)
            setter.set(consumer, fieldValue)
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
