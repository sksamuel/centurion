package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.reflect.KType

/**
 * An [Encoder] typeclass encodes a JVM value of type T into a value suitable
 * for use with Avro.
 *
 * For example, an encoder could encode a String as an instance of [org.apache.avro.util.Utf8],
 * or it could encode it as an instance of [org.apache.avro.generic.GenericFixed].
 *
 * Some encoders use the schema to determine the encoding function to return. For example, Strings
 * can be encoded as [org.apache.avro.util.Utf8]s, [org.apache.avro.generic.GenericFixed]s,
 * [java.nio.ByteBuffer] or [java.lang.String]. Therefore, the [Encoder[String]] typeclass instance
 * uses the schema to select which of these implementations to use.
 *
 * Other types do not require the schema at all. For example, the default Int [Encoder] always
 * returns a [java.lang.Integer] regardless of any schema input.
 */
fun interface Encoder<T> {

   companion object {

      var globalUseJavaString: Boolean = false

      /**
       * Returns an [Encoder] that encodes by simply returning the input value.
       */
      fun <T : Any> identity(): Encoder<T> = object : Encoder<T> {
         override fun encode(schema: Schema, value: T): Any? = value
      }

      fun encoderFor(type: KType): Encoder<*> {
         val encoder: Encoder<*> = when (val classifier = type.classifier) {
            String::class -> if (globalUseJavaString) JavaStringEncoder else StringEncoder
            Boolean::class -> BooleanEncoder
            Float::class -> FloatEncoder
            Double::class -> DoubleEncoder
            Int::class -> IntEncoder
            Long::class -> LongEncoder
            Short::class -> ShortEncoder
            Byte::class -> ByteEncoder
            BigDecimal::class -> BigDecimalStringEncoder
            Set::class -> SetEncoder(encoderFor(type.arguments.first().type!!))
            List::class -> ListEncoder(encoderFor(type.arguments.first().type!!))
            LongArray::class -> LongArrayEncoder()
            IntArray::class -> IntArrayEncoder()
            Map::class -> MapEncoder(encoderFor(type.arguments[1].type!!))
            LocalTime::class -> LocalTimeEncoder
            LocalDateTime::class -> LocalDateTimeEncoder
            Instant::class -> InstantEncoder
            is KClass<*> -> if (classifier.java.isEnum) EnumEncoder<Enum<*>>() else error("Unsupported type $type")
            else -> error("Unsupported type $type")
         }
         return if (type.isMarkedNullable) NullEncoder(encoder) else encoder
      }
   }

   fun encode(schema: Schema, value: T): Any?

   /**
    * Returns an [Encoder<U>] by applying a function [fn] that maps a [U]
    * to an [T], before encoding as an [T] using this encoder.
    */
   fun <U> contraMap(fn: (U) -> T): Encoder<U> {
      val self = this
      return Encoder { schema, value ->
         self.encode(schema, fn(value))
      }
   }
}

