package com.sksamuel.centurion.avro.io.serde

import org.apache.avro.file.CodecFactory
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
   constructor() : this(EncoderFactory.get(), DecoderFactory.get())

   override fun <T : Any> serdeFor(kclass: KClass<T>): Serde<T> {
      return BinarySerde(kclass, encoderFactory, decoderFactory)
   }
}

class DataSerdeFactory(
   private val encoderFactory: EncoderFactory,
   private val decoderFactory: DecoderFactory,
   private val codecFactory: CodecFactory?,
) : SerdeFactory {
   constructor() : this(EncoderFactory.get(), DecoderFactory.get(), null)

   override fun <T : Any> serdeFor(kclass: KClass<T>): Serde<T> {
      return DataSerde(kclass, encoderFactory, decoderFactory, codecFactory)
   }
}

@Suppress("UNCHECKED_CAST")
class CachingSerdeFactory(private val underlying: SerdeFactory) : SerdeFactory {
   private val cache = ConcurrentHashMap<KClass<*>, Serde<*>>()
   override fun <T : Any> serdeFor(kclass: KClass<T>): Serde<T> {
      return cache.getOrPut(kclass) { underlying.serdeFor(kclass) } as Serde<T>
   }
}
