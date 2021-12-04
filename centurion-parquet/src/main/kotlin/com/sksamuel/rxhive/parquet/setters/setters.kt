package com.sksamuel.rxhive.parquet.setters

import com.sksamuel.rxhive.ArrayType
import com.sksamuel.rxhive.BigIntType
import com.sksamuel.rxhive.BinaryType
import com.sksamuel.rxhive.BooleanType
import com.sksamuel.rxhive.CharType
import com.sksamuel.rxhive.DateType
import com.sksamuel.rxhive.DecimalType
import com.sksamuel.rxhive.EnumType
import com.sksamuel.rxhive.Float32Type
import com.sksamuel.rxhive.Float64Type
import com.sksamuel.rxhive.Int16Type
import com.sksamuel.rxhive.Int32Type
import com.sksamuel.rxhive.Int64Type
import com.sksamuel.rxhive.Int8Type
import com.sksamuel.rxhive.MapDataType
import com.sksamuel.rxhive.StringType
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.TimeMicrosType
import com.sksamuel.rxhive.TimeMillisType
import com.sksamuel.rxhive.TimestampMicrosType
import com.sksamuel.rxhive.TimestampMillisType
import com.sksamuel.rxhive.VarcharType
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import java.math.RoundingMode

/**
 * Typeclass for writing values to a [RecordConsumer].
 */
interface Setter {

  fun set(consumer: RecordConsumer, value: Any)

  companion object {
    fun writerFor(type: com.sksamuel.rxhive.Type): Setter {
      return when (type) {
        is StringType -> StringSetter
        is StructType -> StructSetter(type, RoundingMode.UNNECESSARY, false)
        BooleanType -> BooleanSetter
        BinaryType -> BinarySetter
        Float64Type -> DoubleSetter
        Float32Type -> FloatSetter
        Int64Type -> LongSetter
        Int8Type -> IntegerSetter
        Int32Type -> IntegerSetter
        Int16Type -> IntegerSetter
        TimestampMillisType -> TimestampMillisSetter
        TimestampMicrosType -> TODO()
        TimeMicrosType -> TODO()
        TimeMillisType -> TimeMillisSetter
        DateType -> DateSetter
        is MapDataType -> TODO()
        is DecimalType -> TODO()
        is EnumType -> EnumSetter
        is ArrayType -> TODO()
        is CharType -> TODO()
        is VarcharType -> TODO()
        BigIntType -> TODO()
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
  override fun set(consumer: RecordConsumer, value: Any) = consumer.addBinary(Binary.fromReusedByteArray(value.toString().toByteArray()))
}

object FloatSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) = consumer.addFloat(value.toString().toFloat())
}

object BooleanSetter : Setter {
  override fun set(consumer: RecordConsumer, value: Any) = consumer.addBoolean(value == true)
}

