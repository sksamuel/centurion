package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericEnumSymbol

/**
 * An [Encoder] for enum values that encodes data as instances of [GenericEnumSymbol].
 */
class EnumEncoder<T : Enum<*>> : Encoder<T> {
   override fun encode(schema: Schema): (T) -> Any? {
      require(schema.type == Schema.Type.ENUM)
      return { value ->
         val symbol = value as Enum<*>
         GenericData.EnumSymbol(schema, symbol.name)
      }
   }
}
