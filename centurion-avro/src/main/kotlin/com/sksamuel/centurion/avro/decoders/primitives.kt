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
      if (Decoder.useStrictPrimitiveDecoders) return { it as Int }
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

object LongDecoder : Decoder<Long> {
   override fun decode(schema: Schema): (Any?) -> Long {
      require(schema.type == Schema.Type.LONG)
      if (Decoder.useStrictPrimitiveDecoders) return { it as Long }
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

object DoubleDecoder : Decoder<Double> {
   override fun decode(schema: Schema): (Any?) -> Double {
      require(schema.type == Schema.Type.DOUBLE)
      if (Decoder.useStrictPrimitiveDecoders) return { it as Double }
      return { value ->
         when (value) {
            is Double -> value
            is Float -> value.toDouble()
            else -> error("Unsupported value $value")
         }
      }
   }
}

object FloatDecoder : Decoder<Float> {
   override fun decode(schema: Schema): (Any?) -> Float {
      require(schema.type == Schema.Type.FLOAT)
      if (Decoder.useStrictPrimitiveDecoders) return { it as Float }
      return { value ->
         when (value) {
            is Float -> value
            else -> error("Unsupported value $value")
         }
      }
   }
}

object BooleanDecoder : Decoder<Boolean> {
   override fun decode(schema: Schema): (Any?) -> Boolean {
      require(schema.type == Schema.Type.BOOLEAN)
      if (Decoder.useStrictPrimitiveDecoders) return { it as Boolean }
      return { value ->
         when (value) {
            is Boolean -> value
            else -> error("Unsupported value $value")
         }
      }
   }
}
