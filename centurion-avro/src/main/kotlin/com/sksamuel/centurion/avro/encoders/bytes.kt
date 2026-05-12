package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

object ByteBufferEncoder : Encoder<ByteBuffer> {
   override fun encode(schema: Schema, value: ByteBuffer): Any? {
      return when (schema.type) {
         Schema.Type.BYTES -> {
            val bytes = ByteArray(value.remaining())
            value.duplicate().get(bytes)
            bytes
         }
         Schema.Type.FIXED -> FixedByteBufferEncoder.encode(schema, value)
         else -> error("ByteBufferEncoder doesn't support schema type ${schema.type}")
      }
   }
}

object FixedByteBufferEncoder : Encoder<ByteBuffer> {
   override fun encode(schema: Schema, value: ByteBuffer): Any? {
      require(schema.type == Schema.Type.FIXED)
      val remaining = value.remaining()
      val fixedSize = schema.fixedSize
      if (remaining > fixedSize)
         error("Cannot write ByteBuffer with $remaining bytes to fixed type of size $fixedSize")
      val array = ByteArray(fixedSize)
      value.duplicate().get(array, 0, remaining)
      return GenericData.Fixed(schema, array)
   }
}

object ByteArrayEncoder : Encoder<ByteArray> {
   override fun encode(schema: Schema, value: ByteArray): Any? {
      return when (schema.type) {
         Schema.Type.BYTES -> value
         Schema.Type.FIXED -> FixedByteArrayEncoder.encode(schema, value)
         else -> error("ByteArrayEncoder doesn't support schema type ${schema.type}")
      }
   }
}

object FixedByteArrayEncoder : Encoder<ByteArray> {
   override fun encode(schema: Schema, value: ByteArray): Any? {
      require(schema.type == Schema.Type.FIXED)
      val fixedSize = schema.fixedSize
      if (value.size > fixedSize)
         error("Cannot write ByteArray with ${value.size} bytes to fixed type of size $fixedSize")
      val array = ByteArray(fixedSize)
      System.arraycopy(value, 0, array, 0, value.size)
      return GenericData.Fixed(schema, array)
   }
}
