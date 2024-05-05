package com.sksamuel.centurion.avro.io

import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

/**
 * A [CachedReflectionSerdeFactory] will create a [Serde] once for a given type via delegation
 * to a [ReflectionSerdeFactory] and return that cached [Serde] upon future invocations.
 *
 * This instance is thread safe.
 */
object CachedReflectionSerdeFactory {

   private val cache = ConcurrentHashMap<KClass<*>, Serde<*>>()

   /**
    * Creates or returns a [Serde] for the given [kclass].
    */
   fun <T : Any> create(
      kclass: KClass<T>,
      options: SerdeOptions = SerdeOptions()
   ): Serde<T> {
      return cache.getOrPut(kclass) { ReflectionSerdeFactory.create(kclass, options) } as Serde<T>
   }

   /**
    * Creates or returns a [Serde] from the given type parameter [T].
    */
   inline fun <reified T : Any> create(options: SerdeOptions = SerdeOptions()): Serde<T> {
      return create(T::class, options)
   }
}
