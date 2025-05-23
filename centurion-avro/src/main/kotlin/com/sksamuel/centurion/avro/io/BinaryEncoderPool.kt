package com.sksamuel.centurion.avro.io

import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import java.io.OutputStream
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore

class BinaryEncoderPool(
   private val maxSize: Int,
   private val factory: EncoderFactory,
) {

   private val encoders = ConcurrentLinkedQueue<BinaryEncoder>()
   private val semaphore = Semaphore(1)

   fun claim(out: OutputStream): BinaryEncoder {
      semaphore.acquire()
      val reuse = encoders.poll()
      semaphore.release()
      val encoder = factory.binaryEncoder(out, reuse)
      return encoder
   }

   fun <T> use(out: OutputStream, f: (BinaryEncoder) -> T): T {
      val encoder = claim(out)
      try {
         return f(encoder)
      } finally {
         release(encoder)
      }
   }

   fun release(encoder: BinaryEncoder) {
      if (encoders.size < maxSize)
         encoders.offer(encoder)
   }
}
