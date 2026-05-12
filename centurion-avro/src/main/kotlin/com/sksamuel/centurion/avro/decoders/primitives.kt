package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema

object ByteDecoder : Decoder<Byte> {
   override fun decode(schema: Schema, value: Any?): Byte {
      return when (value) {
         is Byte -> value
         else -> error("ByteDecoder cannot decode ${value?.javaClass?.name}: $value")
      }
   }
}

object ShortDecoder : Decoder<Short> {
   override fun decode(schema: Schema, value: Any?): Short {
      return when (value) {
         is Short -> value
         is Byte -> value.toShort()
         else -> error("ShortDecoder cannot decode ${value?.javaClass?.name}: $value")
      }
   }
}

object IntDecoder : Decoder<Int> {
   override fun decode(schema: Schema, value: Any?): Int {
      return when (value) {
         is Int -> value
         is Short -> value.toInt()
         is Byte -> value.toInt()
         else -> error("IntDecoder cannot decode ${value?.javaClass?.name}: $value")
      }
   }
}

object LongDecoder : Decoder<Long> {
   override fun decode(schema: Schema, value: Any?): Long {
      return when (value) {
         is Long -> value
         is Int -> value.toLong()
         is Byte -> value.toLong()
         is Short -> value.toLong()
         else -> error("LongDecoder cannot decode ${value?.javaClass?.name}: $value")
      }
   }
}

object DoubleDecoder : Decoder<Double> {
   override fun decode(schema: Schema, value: Any?): Double {
      return when (value) {
         is Double -> value
         is Float -> value.toDouble()
         else -> error("DoubleDecoder cannot decode ${value?.javaClass?.name}: $value")
      }
   }
}

object FloatDecoder : Decoder<Float> {
   override fun decode(schema: Schema, value: Any?): Float {
      return when (value) {
         is Float -> value
         else -> error("FloatDecoder cannot decode ${value?.javaClass?.name}: $value")
      }
   }
}

object BooleanDecoder : Decoder<Boolean> {
   override fun decode(schema: Schema, value: Any?): Boolean {
      return when (value) {
         is Boolean -> value
         else -> error("BooleanDecoder cannot decode ${value?.javaClass?.name}: $value")
      }
   }
}
