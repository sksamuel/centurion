package com.sksamuel.reactivehive.parquet.setters

import com.sksamuel.reactivehive.ArrayType
import com.sksamuel.reactivehive.BigIntType
import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.CharType
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.DecimalType
import com.sksamuel.reactivehive.EnumType
import com.sksamuel.reactivehive.Float32Type
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.Int16Type
import com.sksamuel.reactivehive.Int32Type
import com.sksamuel.reactivehive.Int64Type
import com.sksamuel.reactivehive.Int8Type
import com.sksamuel.reactivehive.MapDataType
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.TimeMicrosType
import com.sksamuel.reactivehive.TimeMillisType
import com.sksamuel.reactivehive.TimestampMicrosType
import com.sksamuel.reactivehive.TimestampMillisType
import com.sksamuel.reactivehive.VarcharType
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

