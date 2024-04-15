package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import kotlin.reflect.KClass
import kotlin.reflect.KType

/**
 * A [Decoder] typeclass is used to convert an Avro value, such as a [GenericRecord],
 * [SpecificRecord], [GenericFixed], [EnumSymbol], or a basic type, into a specified JVM type.
 *
 * For example, a [Decoder<String>] would convert an input such as a GenericFixed, byte array, or Utf8
 * into a plain JVM [String].
 *
 * Another example, a decoder for nullable types would handle null-based Unions.
 */
fun interface Decoder<T> {

   fun decode(schema: Schema, value: Any?): T

   fun <U> map(fn: (T) -> U): Decoder<U> {
      val self = this
      return Decoder { schema, value -> fn(self.decode(schema, value)) }
   }

   companion object {
      fun decoderFor(type: KType): Decoder<*> {
         val decoder: Decoder<*> = when (val classifier = type.classifier) {
            String::class -> StringDecoder
            Boolean::class -> BooleanDecoder
            Float::class -> FloatDecoder
            Double::class -> DoubleDecoder
            Int::class -> IntDecoder
            Long::class -> LongDecoder
            List::class -> ListDecoder(decoderFor(type.arguments.first().type!!))
            Set::class -> SetDecoder(decoderFor(type.arguments.first().type!!))
            Map::class -> MapDecoder(decoderFor(type.arguments[1].type!!))
            is KClass<*> -> if (classifier.java.isEnum) EnumDecoder(classifier as KClass<out Enum<*>>) else error("Unsupported type $type")
            else -> error("Unsupported type $type")
         }
         return if (type.isMarkedNullable) NullDecoder(decoder) else decoder
      }
   }
}
