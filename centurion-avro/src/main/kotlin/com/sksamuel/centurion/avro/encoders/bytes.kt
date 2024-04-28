package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

object ByteBufferEncoder : Encoder<ByteBuffer> {
   override fun encode(schema: Schema): (ByteBuffer) -> Any? {
      return when (schema.type) {
         Schema.Type.BYTES -> Encoder.identity<ByteBuffer>().encode(schema)
         Schema.Type.FIXED -> FixedByteBufferEncoder.encode(schema)
         else -> error("ByteBufferEncoder doesn't support schema type ${schema.type}")
      }
   }
}

object FixedByteBufferEncoder : Encoder<ByteBuffer> {
   override fun encode(schema: Schema): (ByteBuffer) -> Any? {
      require(schema.type == Schema.Type.FIXED)
      return {
         val array = ByteArray(schema.fixedSize)
         System.arraycopy(it.array(), 0, array, 0, it.array().size)
         GenericData.get().createFixed(null, array, schema)
      }
   }
}

object ByteArrayEncoder : Encoder<ByteArray> {
   override fun encode(schema: Schema): (ByteArray) -> Any? {
      return when (schema.type) {
         Schema.Type.BYTES -> Encoder.identity<ByteArray>().encode(schema)
         Schema.Type.FIXED -> FixedByteArrayEncoder.encode(schema)
         else -> error("ByteArrayEncoder doesn't support schema type ${schema.type}")
      }
   }
}

object FixedByteArrayEncoder : Encoder<ByteArray> {
   override fun encode(schema: Schema): (ByteArray) -> Any? {
      require(schema.type == Schema.Type.FIXED)
      return {
         val array = ByteArray(schema.fixedSize)
         System.arraycopy(it, 0, array, 0, it.size)
         GenericData.get().createFixed(null, array, schema)
      }
   }
}
