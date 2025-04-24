//package com.sksamuel.centurion.avro.io.serde
//
//import com.sksamuel.centurion.avro.io.Format
//import kotlin.reflect.KClass
//
///**
// * A [SerdeFactory] is responsible for creating a [Serde] for a given type and [Format].
// * See [BinarySerde], [DataSerde] and [JsonSerde].
// */
//abstract class SerdeFactory {
//
//   /**
//    * Creates or returns a [Serde] for the given [kclass].
//    */
//   abstract fun <T : Any> create(
//     kclass: KClass<T>,
//     format: Format,
//     options: SerdeOptions,
//   ): Serde<T>
//
//   /**
//    * Creates or returns a [Serde] from the given type parameter [T].
//    */
//   inline fun <reified T : Any> create(format: Format, options: SerdeOptions): Serde<T> {
//      return create(T::class, format, options)
//   }
//}
