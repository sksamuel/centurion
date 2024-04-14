package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

class RecordEncoder : Encoder<Any> {

   override fun encode(schema: Schema, value: Any): Any {
      require(schema.type == Schema.Type.RECORD)

      val kclass = value::class
      require(kclass.isData) { "Can only encode data classes: $kclass" }

      val record = GenericData.Record(schema)

      value::class.declaredMemberProperties.map { member: KProperty1<out Any, *> ->
         val field = schema.getField(member.name)
         val encoder = Encoder.encoderFor(member.returnType) as Encoder<Any?>
         val v = member.getter.call(value)
         val encoded = encoder.encode(field.schema(), v)
         record.put(member.name, encoded)
      }

      return record
   }
}