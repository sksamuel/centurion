package com.sksamuel.centurion.parquet.setters

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
        Schema.Bytes -> BinarySetter
        Schema.Float64 -> DoubleSetter
        Schema.Float32 -> FloatWriter
        Schema.Int64 -> LongSetter
        Schema.Int32 -> IntegerSetter
        Schema.Int16 -> IntegerSetter
        Schema.Int8 -> IntegerSetter
//        TimestampMillisType -> TimestampMillisSetter
//        TimestampMicrosType -> TODO()
//        TimeMicrosType -> TODO()
//        TimeMillisType -> TimeMillisSetter
//        DateType -> DateSetter
//        is MapDataType -> TODO()
//        is DecimalType -> TODO()
//        is EnumType -> EnumSetter
//        is ArrayType -> TODO()
//        is CharType -> TODO()
//        is VarcharType -> TODO()
//        BigIntType -> TODO()
        else -> error("Unsupported writer schema $schema")
      }
    }
  }
}

object BinarySetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) {
    when (value) {
      is ByteArray -> consumer.addBinary(Binary.fromReusedByteArray(value))
      is List<*> -> write(consumer, value.toTypedArray())
    }
  }
}

object IntegerSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addInteger(value.toString().toInt())
}

object LongSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addLong(value.toString().toLong())
}

object DoubleSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addDouble(value.toString().toDouble())
}

object StringWriter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) =
    consumer.addBinary(Binary.fromReusedByteArray(value.toString().toByteArray()))
}

object FloatWriter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addFloat(value.toString().toFloat())
}

object BooleanSetter : Writer {
  override fun write(consumer: RecordConsumer, value: Any) = consumer.addBoolean(value == true)
}

