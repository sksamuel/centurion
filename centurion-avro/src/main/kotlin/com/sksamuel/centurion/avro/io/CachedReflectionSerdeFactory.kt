package com.sksamuel.centurion.avro.io

import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

/**
 * A [CachedReflectionSerdeFactory] will create a [SpecificSerde] once for a given type via delegation
 * to a [ReflectionSerdeFactory] and return that cached [SpecificSerde] upon future invocations.
 *
 * This instance is thread safe.
 */
object CachedReflectionSerdeFactory {

   private val cache = ConcurrentHashMap<KClass<*>, SpecificSerde<*>>()

   /**
    * Creates or returns a [SpecificSerde] for the given [kclass].
    */
   fun <T : Any> create(
      kclass: KClass<T>,
      options: SerdeOptions = SerdeOptions()
   ): SpecificSerde<T> {
      return cache.getOrPut(kclass) { ReflectionSerdeFactory.create(kclass, options) } as SpecificSerde<T>
   }

   /**
    * Creates or returns a [SpecificSerde] from the given type parameter [T].
    */
   inline fun <reified T : Any> create(options: SerdeOptions = SerdeOptions()): SpecificSerde<T> {
      return create(T::class, options)
   }
}
