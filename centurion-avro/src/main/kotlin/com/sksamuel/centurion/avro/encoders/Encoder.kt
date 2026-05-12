package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.schemas.unionNonNullComponent
import org.apache.avro.Schema
import java.math.BigDecimal
import java.nio.ByteBuffer
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
      fun <T : Any> identity(): Encoder<T> = Encoder { schema, value -> value }

      fun encoderFor(type: KType, schema: Schema): Encoder<*> {
         if (type.isMarkedNullable) require(schema.isUnion) { "Require UNION for fields marked nullable" }
         val nonNullSchema = if (schema.isUnion) schema.unionNonNullComponent() else schema
         val firstArgType = type.arguments.firstOrNull()?.type
         val encoder: Encoder<*> = when (val classifier = type.classifier) {
            String::class if nonNullSchema.type == Schema.Type.STRING -> JavaStringEncoder
            String::class if nonNullSchema.type == Schema.Type.BYTES -> ByteStringEncoder
            String::class if nonNullSchema.type == Schema.Type.FIXED -> FixedStringEncoder
            String::class -> StringEncoder
            Boolean::class -> BooleanEncoder
            Float::class -> FloatEncoder
            Double::class -> DoubleEncoder
            Int::class -> IntEncoder
            Long::class -> LongEncoder
            Short::class -> ShortEncoder
            Byte::class -> ByteEncoder
            BigDecimal::class -> BigDecimalStringEncoder
            ByteBuffer::class -> ByteBufferEncoder
            ByteArray::class -> ByteArrayEncoder
            List::class if firstArgType == typeOf<Long>() -> PassThroughListEncoder
            List::class if firstArgType == typeOf<Int>() -> PassThroughListEncoder
            List::class if firstArgType == typeOf<Short>() -> PassThroughListEncoder
            List::class if firstArgType == typeOf<Byte>() -> PassThroughListEncoder
            List::class if firstArgType == typeOf<Boolean>() -> PassThroughListEncoder
            List::class if firstArgType == typeOf<String>() -> PassThroughListEncoder
            List::class if firstArgType == typeOf<Double>() -> PassThroughListEncoder
            List::class if firstArgType == typeOf<Float>() -> PassThroughListEncoder
            List::class -> ListEncoder(encoderFor(firstArgType!!, nonNullSchema.elementType))

            LongArray::class -> LongArrayEncoder()
            IntArray::class -> IntArrayEncoder()
            Set::class if firstArgType == typeOf<Long>() -> PassThroughSetEncoder
            Set::class if firstArgType == typeOf<Int>() -> PassThroughSetEncoder
            Set::class if firstArgType == typeOf<Short>() -> PassThroughSetEncoder
            Set::class if firstArgType == typeOf<Byte>() -> PassThroughSetEncoder
            Set::class if firstArgType == typeOf<Boolean>() -> PassThroughSetEncoder
            Set::class if firstArgType == typeOf<String>() -> PassThroughSetEncoder
            Set::class if firstArgType == typeOf<Double>() -> PassThroughSetEncoder
            Set::class if firstArgType == typeOf<Float>() -> PassThroughSetEncoder
            Set::class -> SetEncoder(encoderFor(firstArgType!!, nonNullSchema.elementType))

            Map::class -> MapEncoder(encoderFor(type.arguments[1].type!!, nonNullSchema.valueType))

            LocalTime::class -> LocalTimeEncoder
            LocalDateTime::class -> LocalDateTimeEncoder
            Instant::class -> InstantEncoder
            is KClass<*> if classifier.java.isEnum -> EnumEncoder()
            is KClass<*> if classifier.isData -> ReflectionRecordEncoder(schema, classifier)
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

