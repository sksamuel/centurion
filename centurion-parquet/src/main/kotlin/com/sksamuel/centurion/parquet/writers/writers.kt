package com.sksamuel.centurion.parquet.writers

import com.sksamuel.centurion.Schema
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import java.math.RoundingMode

/**
 * Typeclass for writing values to a [RecordConsumer].
 */
interface Writer {

  fun write(consumer: RecordConsumer, value: Any)

  companion object {
    fun writerFor(schema: Schema): Writer {
      return when (schema) {
        Schema.Strings -> StringWriter
        is Schema.Struct -> StructWriter(schema, RoundingMode.UNNECESSARY, false)
        Schema.Booleans -> BooleanSetter
        Schema.Bytes -> BinaryWriter
        Schema.Float64 -> DoubleWriter
        Schema.Float32 -> FloatWriter
        Schema.Int64 -> LongWriter
        Schema.Int32 -> IntegerWriter
        Schema.Int16 -> IntegerWriter
        Schema.Int8 -> IntegerWriter
        is Schema.Array -> ArrayWriter(schema)
        is Schema.Nullable -> writerFor(schema.element)
        is Schema.Map -> MapWriter(schema)
        Schema.TimestampMillis -> TimestampMillisWriter
        else -> error("Unsupported writer schema $schema")
      }
    }
  }
}

internal object BinaryWriter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) {
    when (value) {
      is ByteArray -> consumer.addBinary(Binary.fromReusedByteArray(value))
      is List<*> -> write(consumer, Binary.fromReusedByteArray(value.map { it as Byte }.toByteArray()))
    }
  }
}

internal object IntegerWriter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = when (value) {
    is Int -> consumer.addInteger(value)
    else -> consumer.addInteger(value.toString().toInt())
  }
}

internal object LongWriter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addLong(value.toString().toLong())
}

internal object DoubleWriter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addDouble(value.toString().toDouble())
}

internal object StringWriter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) =
    consumer.addBinary(Binary.fromReusedByteArray(value.toString().toByteArray()))
}

internal object FloatWriter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addFloat(value.toString().toFloat())
}

internal object BooleanSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addBoolean(value == true)
}

