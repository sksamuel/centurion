package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol

/**
 * An [Encoder] for enum values that encodes data as instances of [GenericEnumSymbol], or if
 * [Encoder.globalUseJavaStringForEnum] is true, then encodes as Strings.
 */
class EnumEncoder<T : Enum<*>> : Encoder<T> {
   override fun encode(schema: Schema, value: T): Any? {
      require(schema.type == Schema.Type.ENUM)
      val symbol = value as Enum<*>
      return if (Encoder.globalUseJavaStringForEnum)
         symbol.name
      else
         GenericData.get().createEnum(symbol.name, schema)
   }
}
