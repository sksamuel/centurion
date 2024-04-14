package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import java.nio.ByteBuffer
import java.util.UUID

object StringEncoder : Encoder<String> {
   override fun encode(schema: Schema, value: String): Any {
      return when (schema.type) {
         Schema.Type.BYTES -> ByteStringEncoder.encode(schema, value)
         Schema.Type.FIXED -> FixedStringEncoder.encode(schema, value)
         Schema.Type.STRING -> when (schema.getProp(GenericData.STRING_PROP)) {
            "String" -> value
            else -> UTF8StringEncoder.encode(schema, value)
         }

         else -> error("Unsupported type for string schema: $schema")
      }
   }
}

object UUIDEncoder : Encoder<UUID> {
   override fun encode(schema: Schema, value: UUID): Any {
      return Utf8(value.toString())
   }
}

/**
 * An [[Encoder]] for Strings that encodes as avro [[Utf8]]s.
 */
object UTF8StringEncoder : Encoder<String> {
   override fun encode(schema: Schema, value: String): Any {
      return Utf8(value)
   }
}

/**
 * An [Encoder] for Strings that encodes as [ByteBuffer]s.
 */
object ByteStringEncoder : Encoder<String> {
   override fun encode(schema: Schema, value: String): Any {
      return ByteBuffer.wrap(value.encodeToByteArray())
   }
}

/**
 * An [Encoder] for Strings that encodes as [GenericFixed]s.
 */
object FixedStringEncoder : Encoder<String> {
   override fun encode(schema: Schema, value: String): Any {
      val bytes = value.encodeToByteArray()
      if (bytes.size > schema.fixedSize)
         error("Cannot write string with ${bytes.size} bytes to fixed type of size ${schema.fixedSize}")
      return GenericData.get().createFixed(null, bytes, schema)
   }
}
