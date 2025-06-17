package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.time.Instant
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
      fun decoderFor(type: KType, stringType: String?, schema: Schema): Decoder<*> {
         val decoder: Decoder<*> = when (val classifier = type.classifier) {
            // this prop seems to be ignored by the fast reader implementation
            String::class if GenericData.StringType.String.name == stringType -> StringTypeDecoder
            String::class -> StringDecoder
            Boolean::class -> BooleanDecoder
            Float::class -> FloatDecoder
            Double::class -> DoubleDecoder
            Int::class -> IntDecoder
            Long::class -> LongDecoder
            Byte::class -> ByteDecoder
            Short::class -> ShortDecoder
            List::class if type.arguments.first().type == typeOf<Long>() -> PassthroughListDecoder
            List::class if type.arguments.first().type == typeOf<Int>() -> PassthroughListDecoder
            List::class if type.arguments.first().type == typeOf<Short>() -> PassthroughListDecoder
            List::class if type.arguments.first().type == typeOf<Byte>() -> PassthroughListDecoder
            List::class if type.arguments.first().type == typeOf<Boolean>() -> PassthroughListDecoder
            List::class if type.arguments.first().type == typeOf<String>() && GenericData.StringType.String.name == stringType -> PassthroughListDecoder
            List::class -> ListDecoder(decoderFor(type.arguments.first().type!!, stringType, schema.elementType))
            LongArray::class -> LongArrayDecoder(LongDecoder)
            IntArray::class -> IntArrayDecoder(IntDecoder)
            Set::class if type.arguments.first().type == typeOf<Long>() -> PassthroughSetDecoder
            Set::class if type.arguments.first().type == typeOf<Int>() -> PassthroughSetDecoder
            Set::class if type.arguments.first().type == typeOf<Short>() -> PassthroughSetDecoder
            Set::class if type.arguments.first().type == typeOf<Byte>() -> PassthroughSetDecoder
            Set::class if type.arguments.first().type == typeOf<Boolean>() -> PassthroughSetDecoder
            Set::class if type.arguments.first().type == typeOf<String>() && GenericData.StringType.String.name == stringType -> PassthroughSetDecoder
            Set::class -> SetDecoder(decoderFor(type.arguments.first().type!!, stringType, schema.elementType))
            Map::class -> MapDecoder(decoderFor(type.arguments[1].type!!, stringType, schema.valueType))
            LocalTime::class -> LocalTimeDecoder
            Instant::class -> InstantDecoder
            is KClass<*> if classifier.java.isEnum -> EnumDecoder(classifier as KClass<out Enum<*>>)
            is KClass<*> if classifier.isData -> ReflectionRecordDecoder(schema, classifier as KClass<*>)
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
