package com.sksamuel.centurion.avro.decoders

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
 * A [Decoder] typeclass is used to convert an Avro value, such as a [org.apache.avro.generic.GenericRecord],
 * [org.apache.avro.generic.GenericFixed], [org.apache.avro.generic.GenericData.EnumSymbol], and so on,
 * into a specified JVM type.
 *
 * For example, a [Decoder[String]] would convert an input such as a [org.apache.avro.generic.GenericFixed],
 * byte array, or [org.apache.avro.util.Utf8] into a plain JVM [String].
 *
 * Another example, a decoder for nullable types would handle null-based Unions.
 */
fun interface Decoder<T> {

   fun decode(schema: Schema, value: Any?): T

   companion object {

      @Suppress("UNCHECKED_CAST")
      fun decoderFor(type: KType,   schema: Schema): Decoder<*> {
         val nonNullSchema = if (schema.isUnion) schema.unionNonNullComponent() else schema
         val decoder: Decoder<*> = when (val classifier = type.classifier) {
            String::class if schema.type == Schema.Type.STRING -> StringTypeDecoder
            String::class -> StringDecoder
            Boolean::class -> BooleanDecoder
            Float::class -> FloatDecoder
            Double::class -> DoubleDecoder
            Int::class -> IntDecoder
            Long::class -> LongDecoder
            Byte::class -> ByteDecoder
            Short::class -> ShortDecoder
            BigDecimal::class -> BigDecimalStringDecoder
            ByteArray::class -> ByteArrayDecoder
            ByteBuffer::class -> ByteBufferDecoder
            List::class -> {
               val firstArgType = type.arguments.firstOrNull()?.type
               when {
                  firstArgType == typeOf<Long>() -> PassthroughListDecoder
                  firstArgType == typeOf<Int>() -> PassthroughListDecoder
                  firstArgType == typeOf<Short>() -> PassthroughListDecoder
                  firstArgType == typeOf<Byte>() -> PassthroughListDecoder
                  firstArgType == typeOf<Boolean>() -> PassthroughListDecoder
                  firstArgType == typeOf<Double>() -> PassthroughListDecoder
                  firstArgType == typeOf<Float>() -> PassthroughListDecoder
                  firstArgType == typeOf<String>() && nonNullSchema.elementType.type == Schema.Type.STRING -> ListDecoder(StringTypeDecoder)
                  else -> ListDecoder(decoderFor(firstArgType!!, nonNullSchema.elementType))
               }
            }
            LongArray::class -> LongArrayDecoder(LongDecoder)
            IntArray::class -> IntArrayDecoder(IntDecoder)
            Set::class -> {
               val firstArgType = type.arguments.firstOrNull()?.type
               when {
                  firstArgType == typeOf<Long>() -> PassthroughSetDecoder
                  firstArgType == typeOf<Int>() -> PassthroughSetDecoder
                  firstArgType == typeOf<Short>() -> PassthroughSetDecoder
                  firstArgType == typeOf<Byte>() -> PassthroughSetDecoder
                  firstArgType == typeOf<Boolean>() -> PassthroughSetDecoder
                  firstArgType == typeOf<Double>() -> PassthroughSetDecoder
                  firstArgType == typeOf<Float>() -> PassthroughSetDecoder
                  firstArgType == typeOf<String>() && nonNullSchema.elementType.type == Schema.Type.STRING -> SetDecoder(StringTypeDecoder)
                  else -> SetDecoder(decoderFor(firstArgType!!, nonNullSchema.elementType))
               }
            }
            Map::class -> MapDecoder(decoderFor(type.arguments[1].type!!, nonNullSchema.valueType))
            LocalTime::class -> LocalTimeDecoder
            LocalDateTime::class -> LocalDateTimeDecoder
            Instant::class -> InstantDecoder
            is KClass<*> if classifier.java.isEnum -> EnumDecoder(classifier as KClass<out Enum<*>>)
            is KClass<*> if classifier.isData -> ReflectionRecordDecoder(schema, classifier)
            else -> error("Unsupported type $type")
         }
         return if (type.isMarkedNullable) NullDecoder(decoder) else decoder
      }
   }
}

fun <T, U> Decoder<T>.map(fn: (T) -> U): Decoder<U> {
   val self = this
   return Decoder { schema, value ->
      fn(self.decode(schema, value))
   }
}
