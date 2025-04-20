package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.io.Format
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

/**
 * A [CachedSerdeFactory] will create a [Serde] once for a given type via delegation
 * to a [ReflectionSerdeFactory] and return that cached [Serde] upon future invocations. This allows
 * the reflection setup calls to be invoked only once per type, which gives a huge performance gain.
 *
 * This instance is thread safe.
 */
class CachedSerdeFactory(private val factory: SerdeFactory) : SerdeFactory() {

   private val cache = ConcurrentHashMap<KClass<*>, Serde<*>>()

   /**
    * Creates or returns a [Serde] for the given [kclass].
    */
   override fun <T : Any> create(kclass: KClass<T>, format: Format, options: SerdeOptions): Serde<T> {
      return cache.getOrPut(kclass) { factory.create(kclass, format, options) } as Serde<T>
   }
}

