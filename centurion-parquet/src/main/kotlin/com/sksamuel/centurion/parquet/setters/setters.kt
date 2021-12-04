package com.sksamuel.centurion.parquet.setters

import com.sksamuel.centurion.ArrayType
import com.sksamuel.centurion.BigIntType
import com.sksamuel.centurion.BinaryType
import com.sksamuel.centurion.BooleanType
import com.sksamuel.centurion.CharType
import com.sksamuel.centurion.DateType
import com.sksamuel.centurion.DecimalType
import com.sksamuel.centurion.EnumType
import com.sksamuel.centurion.Float32Type
import com.sksamuel.centurion.Float64Type
import com.sksamuel.centurion.Int16Type
import com.sksamuel.centurion.Int32Type
import com.sksamuel.centurion.Int64Type
import com.sksamuel.centurion.Int8Type
import com.sksamuel.centurion.MapDataType
import com.sksamuel.centurion.StringType
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.TimeMicrosType
import com.sksamuel.centurion.TimeMillisType
import com.sksamuel.centurion.TimestampMicrosType
import com.sksamuel.centurion.TimestampMillisType
import com.sksamuel.centurion.VarcharType
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import java.math.RoundingMode

/**
 * Typeclass for writing values to a [RecordConsumer].
 */
interface Setter {

  fun set(consumer: RecordConsumer, value: Any)

  companion object {
    fun writerFor(type: com.sksamuel.centurion.Type): Setter {
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

