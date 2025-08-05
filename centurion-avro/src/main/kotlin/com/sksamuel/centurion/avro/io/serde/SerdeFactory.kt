package com.sksamuel.centurion.avro.io.serde

import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

interface SerdeFactory {
   fun <T : Any> serdeFor(kclass: KClass<T>): Serde<T>
}

class BinarySerdeFactory(
   private val encoderFactory: EncoderFactory,
   private val decoderFactory: DecoderFactory,
) : SerdeFactory {
   override fun <T : Any> serdeFor(kclass: KClass<T>): Serde<T> {
      return BinarySerde(kclass, encoderFactory, decoderFactory)
   }
}

class CachingSerdeFactory(private val underlying: SerdeFactory) : SerdeFactory {

   private val cache = ConcurrentHashMap<KClass<*>, Serde<*>>()

   override fun <T : Any> serdeFor(kclass: KClass<T>): Serde<T> {
      return cache.getOrPut(kclass) { underlying.serdeFor(kclass) } as Serde<T>
   }
}
