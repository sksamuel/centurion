package com.sksamuel.centurion.avro

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.IndexedRecord

object Records {

   fun toRecord(struct: Struct): GenericData.Record {
      val schema = Schemas.toAvro(struct.schema)
      val record = GenericData.Record(schema)
      struct.iterator().forEach { (field, value) ->
         TODO()
//         val encoded = if (value == null) null else Encoders.encoderFor(field.schema).encode(value)
//         record.put(field.name, encoded)
      }
      return record
   }

   fun fromRecord(record: IndexedRecord): Struct {
      val schema = Schemas.fromAvro(record.schema) as Schema.Struct
      val values = schema.fields.withIndex().map { (index, field) ->
         val value = record.get(index)
         val decoder = Decoders.decoderFor(field.schema)
         decoder.decode(value)
      }
      return Struct(schema, values)
   }
}

