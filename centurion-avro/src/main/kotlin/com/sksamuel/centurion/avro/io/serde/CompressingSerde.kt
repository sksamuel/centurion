package com.sksamuel.centurion.avro.io.serde

import org.apache.avro.file.Codec
import java.nio.ByteBuffer

/**
 * A [CompressingSerde] wraps another [Serde] applying compression after serialization,
 * and applying decompression before deserialization.
 *
 * Note: Compression is not super effective on objects with low repeatabilty of strings.
 */
class CompressingSerde<T : Any>(private val codec: Codec, private val serde: Serde<T>) : Serde<T> {

   override fun serialize(obj: T): ByteArray {
      val bytes = serde.serialize(obj)
      val compressed = codec.compress(ByteBuffer.wrap(bytes))
      val b = ByteArray(compressed.remaining())
      compressed.get(b)
      return b
   }

   override fun deserialize(bytes: ByteArray): T {
      return serde.deserialize(codec.decompress(ByteBuffer.wrap(bytes)).array())
   }
}
