package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

/**
 * An [Encoder] that returns a [GenericRecord] for any data class instance at runtime, using
 * reflection to access the fields of the data class.
 *
 * The [ReflectionRecordEncoder] is generic, but slower than [SpecificRecordEncoder] which can
 * pre-create some of the reflection calls needed in advance.
 */
class ReflectionRecordEncoder : Encoder<Any> {

   override fun encode(schema: Schema, value: Any): Any? {
      return SpecificRecordEncoder(value::class as KClass<Any>).encode(schema, value)
   }
}

/**
 * An [Encoder] that returns a [GenericRecord] for a given data class instance, using
 * reflection to access the fields of the data class.
 *
 * In contrast to [ReflectionRecordEncoder], this encoder requires the class in advance,
 * which it uses to pre-generate some of the reflective calls needed. This approach is faster,
 * but a new encoder must be created for each data class.
 */
class SpecificRecordEncoder<T : Any>(
   private val kclass: KClass<T>,
) : Encoder<T> {

   companion object {
      inline operator fun <reified T : Any> invoke() = SpecificRecordEncoder(T::class)
   }

   init {
      require(kclass.isData) { "Can only encode data classes: $kclass" }
   }

   override fun encode(schema: Schema, value: T): Any? {
      require(schema.type == Schema.Type.RECORD) { "Provided schema must be a RECORD" }

      val encoders = kclass.declaredMemberProperties.map { member: KProperty1<out Any, *> ->
         val field = schema.getField(member.name) ?: error("Could not find field ${member.name} in schema")
         val encoder = Encoder.encoderFor(member.returnType) as Encoder<Any?>
         Triple(encoder, member.getter, field.pos())
      }

      val record = GenericData.Record(schema)
      encoders.map { (encoder, getter, pos) ->
         val fieldSchema = schema.fields[pos].schema()
         val value = getter.call(value)
         val encoded = encoder.encode(fieldSchema, value)
         record.put(pos, encoded)
      }
      return record
   }
}

