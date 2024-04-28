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
   override fun encode(schema: Schema): (String) -> Any? {
      if (Encoder.globalUseJavaString || schema.getProp(GenericData.STRING_PROP) == "String")
         return JavaStringEncoder.encode(schema)

      return when (schema.type) {
         Schema.Type.STRING -> UTF8StringEncoder.encode(schema)
         Schema.Type.BYTES -> ByteStringEncoder.encode(schema)
         Schema.Type.FIXED -> FixedStringEncoder.encode(schema)
         else -> error("Unsupported type for string schema: $schema")
      }
   }
}

/**
 * An [Encoder] for strings which always uses the JVM String object regardless
 * of any [GenericData.STRING_PROP] settings on the schema.
 */
object JavaStringEncoder : Encoder<String> {
   override fun encode(schema: Schema): (String) -> Any? = { it }
}

/**
 * An [Encoder] for UUID that encodes as avro [Utf8]s.
 */
object Utf8UUIDEncoder : Encoder<UUID> {
   override fun encode(schema: Schema): (UUID) -> Any? = { Utf8(it.toString()) }
}

/**
 * An [Encoder] for UUID that encodes as JVM Strings.
 */
object JavaStringUUIDEncoder : Encoder<UUID> {
   override fun encode(schema: Schema): (UUID) -> Any? = { it.toString() }
}

/**
 * An [Encoder] for Strings that encodes as avro [Utf8]s.
 */
object UTF8StringEncoder : Encoder<String> {
   override fun encode(schema: Schema): (String) -> Any? = { Utf8(it) }
}

/**
 * An [Encoder] for Strings that encodes as [ByteBuffer]s.
 */
object ByteStringEncoder : Encoder<String> {
   override fun encode(schema: Schema): (String) -> Any? {
      return { ByteBuffer.wrap(it.encodeToByteArray()) }
   }
}

/**
 * An [Encoder] for Strings that encodes as [GenericFixed]s.
 */
object FixedStringEncoder : Encoder<String> {
   override fun encode(schema: Schema): (String) -> Any? {
      return { value ->
         val bytes = value.encodeToByteArray()
         if (bytes.size > schema.fixedSize)
            error("Cannot write string with ${bytes.size} bytes to fixed type of size ${schema.fixedSize}")
         GenericData.get().createFixed(null, bytes, schema)
      }
   }
}
