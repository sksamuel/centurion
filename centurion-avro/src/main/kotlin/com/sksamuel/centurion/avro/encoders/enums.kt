package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

class EnumEncoder<T : Enum<*>> : Encoder<T> {
   override fun encode(schema: Schema, value: T): Any? {
      require(schema.type == Schema.Type.ENUM)
      val symbol = value as Enum<*>
      return GenericData.get().createEnum(symbol.name, schema)
   }
}
