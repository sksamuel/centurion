package com.sksamuel.centurion.avro.encoders

import io.kotest.core.Tuple4
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

/**
 * An [Encoder] that returns a [GenericRecord] for any data class instance at runtime, using
 * reflection to access the fields of the data class.
 *
 * The [ReflectionRecordEncoder] is generic, but slower than [SpecificRecordEncoder] which can
 * pre-create some of the reflection calls needed, at the cost of needing to know the schema in advance.
 *
 * See [CachedSpecificRecordEncoder].
 */
class ReflectionRecordEncoder : Encoder<Any> {

   override fun encode(schema: Schema, value: Any): Any {
      return SpecificRecordEncoder(value::class as KClass<Any>, schema).encode(schema, value)
   }
}

/**
 * An [Encoder] that returns a [GenericRecord] for any data class instance at runtime, using
 * reflection to access the fields of the data class.
 *
 * This encoder maintains a map of [SpecificRecordEncoder]s which are created on demand.
 */
class CachedSpecificRecordEncoder : Encoder<Any> {

   private val encoders = ConcurrentHashMap<String, SpecificRecordEncoder<Any>>()

   override fun encode(schema: Schema, value: Any): Any {
      return encoders.getOrPut(schema.fullName) {
         SpecificRecordEncoder(value::class as KClass<Any>, schema)
      }.encode(schema, value)
   }
}

/**
 * An [Encoder] that returns a [GenericRecord] for a given data class instance, using
 * reflection to access the fields of the data class.
 *
 * In contrast to [ReflectionRecordEncoder], this encoder requires the class and schema in advance,
 * which it uses to pre-generate some of the reflective calls needed. This approach is faster,
 * but a new encoder must be created for each data class.
 *
 * See [CachedSpecificRecordEncoder].
 */
class SpecificRecordEncoder<T : Any>(
   private val kclass: KClass<T>,
   private val schema: Schema,
) : Encoder<T> {

   companion object {
      inline operator fun <reified T : Any> invoke(schema: Schema) = SpecificRecordEncoder(T::class, schema)
   }

   init {
      require(kclass.isData) { "Can only encode data classes: $kclass" }
      require(schema.type == Schema.Type.RECORD) { "Provided schema must be a RECORD" }
   }

   private val encoders = kclass.declaredMemberProperties.map { member: KProperty1<out Any, *> ->
      val field = schema.getField(member.name) ?: error("Could not find field ${member.name} in schema")
      val encoder = Encoder.encoderFor(member.returnType) as Encoder<Any?>
      Tuple4(field.schema(), encoder, member.name, member.getter)
   }

   override fun encode(schema: Schema, value: T): GenericRecord {
      require(this.schema.fullName == schema.fullName) { "Provided schema must match schema used to create this class" }

      val record = GenericData.Record(schema)

      encoders.map { (schema, encoder, name, getter) ->
         val v = getter.call(value)
         val encoded = encoder.encode(schema, v)
         record.put(name, encoded)
      }

      return record
   }
}
