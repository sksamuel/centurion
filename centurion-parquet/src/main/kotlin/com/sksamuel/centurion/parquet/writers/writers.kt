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
        Schema.Float64 -> DoubleSetter
        Schema.Float32 -> FloatWriter
        Schema.Int64 -> LongSetter
        Schema.Int32 -> IntegerSetter
        Schema.Int16 -> IntegerSetter
        Schema.Int8 -> IntegerSetter
        is Schema.Array -> ArrayWriter(schema)
//        TimestampMillisType -> TimestampMillisSetter
//        TimeMillisType -> TimeMillisSetter
//        DateType -> DateSetter
//        is EnumType -> EnumSetter
        else -> error("Unsupported writer schema $schema")
      }
    }
  }
}

internal object BinaryWriter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) {
    when (value) {
      is ByteArray -> consumer.addBinary(Binary.fromReusedByteArray(value))
      is List<*> -> write(consumer, value.toTypedArray())
    }
  }
}

internal object IntegerSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addInteger(value.toString().toInt())
}

internal object LongSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addLong(value.toString().toLong())
}

internal object DoubleSetter : Writer {
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

