package com.sksamuel.centurion.avro

import org.apache.avro.Schema

/**
 * An [Encoder] typeclass encodes a JVM value of type T into a value suitable
 * for use with Avro.
 *
 * For example, an encoder could encode a String as an instance of [Utf8],
 * or it could encode it as an instance of [GenericFixed].
 *
 * Some encoders use the schema to determine the encoding function to return. For example, strings
 * can be encoded as [UTF8]s, [GenericFixed]]s, [ByteBuffers] or [java.lang.String]s.
 * Therefore, the []Encoder<String>] typeclass instances uses the schema to select which of these
 * implementations to use.
 *
 * Other types may not require the schema at all. For example, the default [Encoder<Int>] always
 * returns a java.lang.Integer regardless of any schema input.
 */
fun interface Encoder<T> {

   companion object {
      /**
       * Returns an [Encoder] that encodes using the supplied function.
       */
      operator fun <T> invoke(f: (T) -> Any) = Encoder<T> { _, value -> f(value) }

      /**
       * Returns an [Encoder] that encodes by simply returning the input value.
       */
      fun <T : Any> identity(): Encoder<T> = Encoder { _, value -> value }
   }

   fun encode(schema: Schema, value: T): Any

   /**
    * Returns an [Encoder<U>] by applying a function [fn] that maps a [U]
    * to an [T], before encoding as an [T] using this encoder.
    */
   fun <U> contraMap(fn: (U) -> T): Encoder<U> = object : Encoder<U> {
      override fun encode(schema: Schema, value: U): Any {
         return this@Encoder.encode(schema, fn(value))
      }
   }
}

