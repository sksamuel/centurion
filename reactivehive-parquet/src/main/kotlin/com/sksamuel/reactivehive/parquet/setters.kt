package com.sksamuel.reactivehive.parquet

import com.sksamuel.reactivehive.ArrayType
import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.ByteType
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.DecimalType
import com.sksamuel.reactivehive.DoubleType
import com.sksamuel.reactivehive.EnumType
import com.sksamuel.reactivehive.FloatType
import com.sksamuel.reactivehive.IntType
import com.sksamuel.reactivehive.LongType
import com.sksamuel.reactivehive.MapDataType
import com.sksamuel.reactivehive.ShortType
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TimeMicrosType
import com.sksamuel.reactivehive.TimeMillisType
import com.sksamuel.reactivehive.TimestampMicrosType
import com.sksamuel.reactivehive.TimestampMillisType
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import java.math.RoundingMode

/**
 * Typeclass for writing values to a [RecordConsumer].
 */
interface Setter {

  fun set(consumer: RecordConsumer, value: Any)

  companion object {
    fun writerFor(type: com.sksamuel.reactivehive.Type): Setter {
      return when (type) {
        is StringType -> StringSetter
        is StructType -> StructSetter(type, RoundingMode.UNNECESSARY, false)
        BooleanType -> BooleanSetter
        BinaryType -> BinarySetter
        DoubleType -> DoubleSetter
        FloatType -> FloatSetter
        LongType -> LongSetter
        ByteType -> IntegerSetter
        IntType -> IntegerSetter
        ShortType -> IntegerSetter
        TimestampMillisType -> TODO()
        TimestampMicrosType -> TODO()
        TimeMicrosType -> TODO()
        TimeMillisType -> TODO()
        DateType -> TODO()
        is MapDataType -> TODO()
        is DecimalType -> TODO()
        is EnumType -> TODO()
        is ArrayType -> TODO()
      }
    }
  }
}

object BinarySetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) {
    when (value) {
      is ByteArray -> consumer.addBinary(Binary.fromReusedByteArray(value))
      is List<*> -> set(consumer, value.toTypedArray())
    }
  }
}

object IntegerSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) = consumer.addInteger(value.toString().toInt())
}

object LongSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) = consumer.addLong(value.toString().toLong())
}

object DoubleSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) = consumer.addDouble(value.toString().toDouble())
}

object StringSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) = consumer.addBinary(Binary.fromString(value.toString()))
}

object FloatSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) = consumer.addFloat(value.toString().toFloat())
}

object BooleanSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) = consumer.addBoolean(value == true)
}

class StructSetter(private val type: StructType,
                   roundingMode: RoundingMode,
    // set to true when the initial root message, otherwise false for GroupType
                   private val root: Boolean) : Setter {

  override fun set(consumer: RecordConsumer, value: Any) {

    // in parquet, nested types must be wrapped in "groups"
    fun wrapInGroup(fn: () -> Unit) {
      consumer.startGroup()
      fn()
      consumer.endGroup()
    }

    // top level types must be wrapped in messages
    fun wrapInMessage(fn: () -> Unit) {
      consumer.startMessage()
      fn()
      consumer.endMessage()
    }

    fun write() {

      val values = when (value) {
        is Struct -> value.values
        else -> throw UnsupportedOperationException("$value is not recognized as a struct type")
      }

      type.fields.forEachIndexed { k, field ->
        val fieldValue = values[k]
        // null values are handled in parquet by skipping them completely when writing out
        if (fieldValue != null) {
          consumer.startField(field.name, k)
          val writer = Setter.writerFor(field.type)
          writer.set(consumer, fieldValue)
          consumer.endField(field.name, k)
        }
      }
    }

    if (root) {
      wrapInMessage { write() }
    } else {
      wrapInGroup { write() }
    }
  }
}