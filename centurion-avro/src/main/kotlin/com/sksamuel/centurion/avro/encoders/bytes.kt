package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

object ByteBufferEncoder : Encoder<ByteBuffer> {
   override fun encode(schema: Schema, value: ByteBuffer): Any? {
      return when (schema.type) {
         Schema.Type.BYTES -> Encoder.identity()
         Schema.Type.FIXED -> FixedByteBufferEncoder
         else -> error("ByteBufferEncoder doesn't support schema type ${schema.type}")
      }
   }
}

object FixedByteBufferEncoder : Encoder<ByteBuffer> {
   override fun encode(schema: Schema, value: ByteBuffer): Any? {
      require(schema.type == Schema.Type.FIXED)
      val array = ByteArray(schema.fixedSize)
      System.arraycopy(value.array(), 0, array, 0, value.array().size)
      return GenericData.get().createFixed(null, array, schema)
   }
}

object ByteArrayEncoder : Encoder<ByteArray> {
   override fun encode(schema: Schema, value: ByteArray): Any? {
      return when (schema.type) {
         Schema.Type.BYTES -> Encoder.identity<ByteArray>()
         Schema.Type.FIXED -> FixedByteArrayEncoder
         else -> error("ByteArrayEncoder doesn't support schema type ${schema.type}")
      }
   }
}

object FixedByteArrayEncoder : Encoder<ByteArray> {
   override fun encode(schema: Schema, value: ByteArray): Any? {
      require(schema.type == Schema.Type.FIXED)
      val array = ByteArray(schema.fixedSize)
      System.arraycopy(value, 0, array, 0, value.size)
      return GenericData.get().createFixed(null, array, schema)
   }
}
