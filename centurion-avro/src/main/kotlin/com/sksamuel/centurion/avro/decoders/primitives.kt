package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema

object ByteDecoder : Decoder<Byte> {
   override fun decode(schema: Schema): (Any?) -> Byte {
      require(schema.type == Schema.Type.BYTES)
      return { value ->
         when (value) {
            is Byte -> value
            else -> error("Unsupported value $value")
         }
      }
   }
}

object ShortDecoder : Decoder<Short> {
   override fun decode(schema: Schema): (Any?) -> Short {
      require(schema.type == Schema.Type.INT)
      return { value ->
         when (value) {
            is Short -> value
            is Byte -> value.toShort()
            else -> error("Unsupported value $value")
         }
      }
   }
}

object IntDecoder : Decoder<Int> {
   override fun decode(schema: Schema): (Any?) -> Int {
      require(schema.type == Schema.Type.INT)
      return { value ->
         when (value) {
            is Int -> value
            is Short -> value.toInt()
            is Byte -> value.toInt()
            else -> error("Unsupported value $value")
         }
      }
   }
}

object StrictLongDecoder : Decoder<Long> {
   override fun decode(schema: Schema): (Any?) -> Long = { it as Long }
}

object LongDecoder : Decoder<Long> {
   override fun decode(schema: Schema): (Any?) -> Long {
      require(schema.type == Schema.Type.LONG)
      return { value ->
         when (value) {
            is Long -> value
            is Int -> value.toLong()
            is Byte -> value.toLong()
            is Short -> value.toLong()
            else -> error("Unsupported value $value")
         }
      }
   }
}

object StrictIntDecoder : Decoder<Int> {
   override fun decode(schema: Schema): (Any?) -> Int = { it as Int }
}

object DoubleDecoder : Decoder<Double> {
   override fun decode(schema: Schema): (Any?) -> Double {
      require(schema.type == Schema.Type.DOUBLE)
      return { value ->
         when (value) {
            is Double -> value
            is Float -> value.toDouble()
            else -> error("Unsupported value $value")
         }
      }
   }
}

object StrictDoubleDecoder : Decoder<Double> {
   override fun decode(schema: Schema): (Any?) -> Double = { it as Double }
}

object FloatDecoder : Decoder<Float> {
   override fun decode(schema: Schema): (Any?) -> Float {
      require(schema.type == Schema.Type.FLOAT)
      return { value ->
         when (value) {
            is Float -> value
            else -> error("Unsupported value $value")
         }
      }
   }
}

object StrictFloatDecoder : Decoder<Float> {
   override fun decode(schema: Schema): (Any?) -> Float = { it as Float }
}

object BooleanDecoder : Decoder<Boolean> {
   override fun decode(schema: Schema): (Any?) -> Boolean {
      require(schema.type == Schema.Type.BOOLEAN)
      return { value ->
         when (value) {
            is Boolean -> value
            else -> error("Unsupported value $value")
         }
      }
   }
}

object StrictBooleanDecoder : Decoder<Boolean> {
   override fun decode(schema: Schema): (Any?) -> Boolean = { it as Boolean }
}
