package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.io.Format
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

/**
 * A [CachedSerdeFactory] will create a [Serde] once per type by delegating to a provided [SerdeFactory].
 * Upon subsequent requests for the same type, it will return the cached instance.
 *
 * This instance is thread safe.
 */
class CachedSerdeFactory(private val factory: SerdeFactory) : SerdeFactory() {

   private val cache = ConcurrentHashMap<KClass<*>, Serde<*>>()

   /**
    * Creates or returns a [Serde] for the given [kclass].
    */
   @Suppress("UNCHECKED_CAST")
   override fun <T : Any> create(kclass: KClass<T>, format: Format, options: SerdeOptions): Serde<T> {
      return cache.getOrPut(kclass) { factory.create(kclass, format, options) } as Serde<T>
   }
}

