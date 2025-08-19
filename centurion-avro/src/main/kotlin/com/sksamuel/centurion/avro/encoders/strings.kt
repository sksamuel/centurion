package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import java.util.UUID

/**
 * An [Encoder] for strings which always uses the schema to determine if a string should be stored as
 * bytes, fixed, utf8 or a java String type.
 */
object StringEncoder : Encoder<String> {
   override fun encode(schema: Schema, value: String): Any? {
      return when (schema.type) {
         Schema.Type.STRING -> UTF8StringEncoder.encode(schema, value)
         Schema.Type.BYTES -> ByteStringEncoder.encode(schema, value)
         Schema.Type.FIXED -> FixedStringEncoder.encode(schema, value)
         else -> error("Unsupported type for string schema: $schema")
      }
   }
}

/**
 * An [Encoder] for strings which always uses the JVM String object regardless
 * of any [GenericData.STRING_PROP] settings on the schema.
 */
object JavaStringEncoder : Encoder<String> {
   override fun encode(schema: Schema, value: String): String = value
}

/**
 * An [Encoder] for UUID that encodes as avro [Utf8]s.
 */
object Utf8UUIDEncoder : Encoder<UUID> {
   override fun encode(schema: Schema, value: UUID): Utf8 = Utf8(value.toString())
}

/**
 * An [Encoder] for UUID that encodes as JVM Strings.
 */
object JavaStringUUIDEncoder : Encoder<UUID> {
   override fun encode(schema: Schema, value: UUID): String = value.toString()
}

/**
 * An [Encoder] for Strings that encodes as avro [Utf8]s.
 */
object UTF8StringEncoder : Encoder<String> {
   override fun encode(schema: Schema, value: String): Utf8 = Utf8(value)
}

/**
 * An [Encoder] for Strings that encodes as [ByteBuffer]s.
 */
object ByteStringEncoder : Encoder<String> {
   override fun encode(schema: Schema, value: String): ByteBuffer {
      return ByteBuffer.wrap(value.encodeToByteArray())
   }
}

/**
 * An [Encoder] for Strings that encodes as [org.apache.avro.generic.GenericFixed]s.
 */
object FixedStringEncoder : Encoder<String> {
   override fun encode(schema: Schema, value: String): Any? {
      val bytes = value.encodeToByteArray()
      if (bytes.size > schema.fixedSize)
         error("Cannot write string with ${bytes.size} bytes to fixed type of size ${schema.fixedSize}")
      return GenericData.get().createFixed(null, bytes, schema)
   }
}
