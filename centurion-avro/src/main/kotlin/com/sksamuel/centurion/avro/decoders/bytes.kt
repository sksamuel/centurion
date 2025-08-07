package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

object ByteArrayDecoder : Decoder<ByteArray> {
   override fun decode(schema: Schema, value: Any?): ByteArray {
      return when (value) {
         is ByteArray -> value
         is GenericData.Fixed -> value.bytes()
         is ByteBuffer -> {
            val array = ByteArray(value.remaining())
            value.get(array)
            array
         }
         else -> error("Cannot decode ${value?.javaClass} as ByteArray")
      }
   }
}

object ByteBufferDecoder : Decoder<ByteBuffer> {
   override fun decode(schema: Schema, value: Any?): ByteBuffer {
      return when (value) {
         is ByteArray -> ByteBuffer.wrap(value)
         is GenericData.Fixed -> ByteBuffer.wrap(value.bytes())
         is ByteBuffer -> {
            val array = ByteArray(value.remaining())
            value.get(array)
            ByteBuffer.wrap(array)
         }
         else -> error("Cannot decode ${value?.javaClass} as ByteBuffer")
      }
   }
}
