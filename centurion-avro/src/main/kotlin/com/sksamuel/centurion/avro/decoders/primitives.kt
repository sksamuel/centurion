package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema

object ByteDecoder : Decoder<Byte> {
   override fun decode(schema: Schema, value: Any?): Byte {
      require(schema.type == Schema.Type.BYTES)
      return when (value) {
         is Byte -> value
         else -> error("Unsupported value $value")
      }
   }
}

object ShortDecoder : Decoder<Short> {
   override fun decode(schema: Schema, value: Any?): Short {
      require(schema.type == Schema.Type.INT)
      return when (value) {
         is Byte -> value.toShort()
         is Short -> value
         else -> error("Unsupported value $value")
      }
   }
}

object IntDecoder : Decoder<Int> {
   override fun decode(schema: Schema, value: Any?): Int {
      require(schema.type == Schema.Type.INT)
      return when (value) {
         is Byte -> value.toInt()
         is Short -> value.toInt()
         is Int -> value
         else -> error("Unsupported value $value")
      }
   }
}

object LongDecoder : Decoder<Long> {
   override fun decode(schema: Schema, value: Any?): Long {
      require(schema.type == Schema.Type.LONG)
      return when (value) {
         is Byte -> value.toLong()
         is Short -> value.toLong()
         is Int -> value.toLong()
         is Long -> value
         else -> error("Unsupported value $value")
      }
   }
}

object DoubleDecoder : Decoder<Double> {
   override fun decode(schema: Schema, value: Any?): Double {
      require(schema.type == Schema.Type.DOUBLE)
      return when (value) {
         is Float -> value.toDouble()
         is Double -> value
         else -> error("Unsupported value $value")
      }
   }
}

object FloatDecoder : Decoder<Float> {
   override fun decode(schema: Schema, value: Any?): Float {
      require(schema.type == Schema.Type.FLOAT)
      return when (value) {
         is Float -> value
         else -> error("Unsupported value $value")
      }
   }
}

object BooleanDecoder : Decoder<Boolean> {
   override fun decode(schema: Schema, value: Any?): Boolean {
      require(schema.type == Schema.Type.BOOLEAN)
      return when (value) {
         is Boolean -> value
         else -> error("Unsupported value $value")
      }
   }
}
