package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.typeOf

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

      /**
       * Returns an [Encoder] that encodes by simply returning the input value.
       */
      fun <T : Any> identity(): Encoder<T> = object : Encoder<T> {
         override fun encode(schema: Schema, value: T): Any? = value
      }

      fun encoderFor(type: KType, stringType: String?): Encoder<*> {
         val encoder: Encoder<*> = when (val classifier = type.classifier) {
            String::class if GenericData.StringType.String.name == stringType -> JavaStringEncoder
            String::class -> StringEncoder
            Boolean::class -> BooleanEncoder
            Float::class -> FloatEncoder
            Double::class -> DoubleEncoder
            Int::class -> IntEncoder
            Long::class -> LongEncoder
            Short::class -> ShortEncoder
            Byte::class -> ByteEncoder
            BigDecimal::class -> BigDecimalStringEncoder
            List::class if type.arguments.first().type == typeOf<Long>() -> PassThroughListEncoder
            List::class if type.arguments.first().type == typeOf<Int>() -> PassThroughListEncoder
            List::class if type.arguments.first().type == typeOf<Short>() -> PassThroughListEncoder
            List::class if type.arguments.first().type == typeOf<Byte>() -> PassThroughListEncoder
            List::class if type.arguments.first().type == typeOf<Boolean>() -> PassThroughListEncoder
            List::class if type.arguments.first().type == typeOf<String>() && GenericData.StringType.String.name == stringType -> PassThroughListEncoder
            List::class -> ListEncoder(encoderFor(type.arguments.first().type!!, stringType))
            LongArray::class -> LongArrayEncoder()
            IntArray::class -> IntArrayEncoder()
            Set::class if type.arguments.first().type == typeOf<Long>() -> PassThroughSetEncoder
            Set::class if type.arguments.first().type == typeOf<Int>() -> PassThroughSetEncoder
            Set::class if type.arguments.first().type == typeOf<Short>() -> PassThroughSetEncoder
            Set::class if type.arguments.first().type == typeOf<Byte>() -> PassThroughSetEncoder
            Set::class if type.arguments.first().type == typeOf<Boolean>() -> PassThroughSetEncoder
            Set::class if type.arguments.first().type == typeOf<String>() && GenericData.StringType.String.name == stringType -> PassThroughSetEncoder
            Set::class -> SetEncoder(encoderFor(type.arguments.first().type!!, stringType))
            Map::class -> MapEncoder(encoderFor(type.arguments[1].type!!, stringType))
            LocalTime::class -> LocalTimeEncoder
            LocalDateTime::class -> LocalDateTimeEncoder
            Instant::class -> InstantEncoder
            is KClass<*> if classifier.java.isEnum -> EnumEncoder<Enum<*>>()
            is KClass<*> if classifier.isData -> ReflectionRecordEncoder<Any>()
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

