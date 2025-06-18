package com.sksamuel.centurion.avro.encoders

import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import java.io.ByteArrayOutputStream

class BinaryEncoderPooledObjectFactory(
   private val encoderFactory: EncoderFactory
) : BasePooledObjectFactory<BinaryEncoder>() {

   private val dump = ByteArrayOutputStream()

   override fun create(): BinaryEncoder {
      return encoderFactory.binaryEncoder(dump, null)
   }

   override fun wrap(obj: BinaryEncoder): PooledObject<BinaryEncoder> {
      return DefaultPooledObject(obj)
   }
}
