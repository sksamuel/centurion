package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer

/**
 * A [Decoder] for [String]s that pattern matches on the incoming type to decode.
 *
 * The schema is not referenced, meaning this decoder is forgiving of types that do not conform to
 * the schema, but are nevertheless valid representations of strings.
 */
object StringDecoder : Decoder<String> {

   override fun decode(schema: Schema, value: Any?): String {
      return when (value) {
         is CharSequence -> value.toString() // covers String and Utf8 as well
         is ByteArray -> Utf8(value).toString()
         is ByteBuffer -> Utf8(value.array()).toString()
         is GenericFixed -> Utf8(value.bytes()).toString()
         else -> error("Unsupported type $value")
      }
   }
}

/**
 * A [Decoder] for [CharSequence] that pattern matches on the incoming type to decode.
 *
 * The schema is not used, meaning this decoder is forgiving of types that do not conform to
 * the schema, but are nevertheless coerable to strings.
 */
val CharSequenceDecoder: Decoder<CharSequence> = StringDecoder.map { it }

/**
 * A [Decoder] for [Utf8] that pattern matches on the incoming type to decode.
 *
 * The schema is not used, meaning this decoder is forgiving of types that do not conform to
 * the schema, but are nevertheless coerable to [Utf8]s.
 */
object UTF8Decoder : Decoder<Utf8> {
   override fun decode(schema: Schema, value: Any?): Utf8 {
      return when (value) {
         is CharSequence -> Utf8(value.toString())
         is ByteArray -> Utf8(value)
         is ByteBuffer -> Utf8(value.array())
         is GenericFixed -> Utf8(value.bytes())
         else -> error("Unsupported type $value")
      }
   }
}

/**
 * A [Decoder] for [String]s that uses the schema to determine the expected type.
 *
 * Unlike [StringDecoder] this decoder is not tolerant of data that does not match the expected
 * encoding from the schema. Eg, if the schema specifies FIXED but the value is a String, then this
 * will throw an error.
 */
object StrictStringDecoder : Decoder<String> {
   override fun decode(schema: Schema, value: Any?): String {
      return when (schema.type) {
         Schema.Type.STRING -> when (value) {
            is String -> value
            is Utf8 -> value.toString()
            else -> error("Unsupported type for string schema: $schema")
         }

         Schema.Type.BYTES -> ByteStringDecoder.decode(schema, value)
         Schema.Type.FIXED -> GenericFixedStringDecoder.decode(schema, value)
         else -> error("Unsupported type for string schema: $schema")
      }
   }
}

/**
 * A [Decoder] for Strings that decodes from [ByteBuffer]s and [ByteArray]s.
 */
object ByteStringDecoder : Decoder<String> {
   override fun decode(schema: Schema, value: Any?): String {
      require(schema.type == Schema.Type.BYTES)
      return when (value) {
         is ByteArray -> Utf8(value).toString()
         is ByteBuffer -> Utf8(value.array()).toString()
         else -> error("This decoder expects bytes but was $value")
      }
   }
}

/**
 * A [Decoder] for Strings that decodes from [GenericFixed]s.
 */
object GenericFixedStringDecoder : Decoder<String> {
   override fun decode(schema: Schema, value: Any?): String {
      require(schema.type == Schema.Type.FIXED)
      return when (value) {
         is GenericFixed -> Utf8(value.bytes()).toString()
         else -> error("This decoder expects GenericFixed but was $value")
      }
   }
}
