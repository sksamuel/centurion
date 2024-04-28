package com.sksamuel.centurion.avro.encoders

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
 * pre-create some of the reflection calls needed in advance.
 *
 * See [CachedSpecificRecordEncoder].
 */
class ReflectionRecordEncoder : Encoder<Any> {

   override fun encode(schema: Schema): (Any) -> Any? {
      return { value -> SpecificRecordEncoder(value::class as KClass<Any>).encode(schema).invoke(value) }
   }
}

/**
 * An [Encoder] that returns a [GenericRecord] for any data class instance at runtime, using
 * reflection to access the fields of the data class.
 *
 * This encoder maintains a map of [SpecificRecordEncoder]s which are created on demand.
 */
class CachedSpecificRecordEncoder : Encoder<Any> {

   private val encoders = ConcurrentHashMap<String, (Any) -> Any?>()

   override fun encode(schema: Schema): (Any) -> Any? {
      return { value ->
         encoders.getOrPut(schema.fullName) {
            { SpecificRecordEncoder(value::class as KClass<Any>).encode(schema) }
         }.invoke(value)
      }
   }
}

/**
 * An [Encoder] that returns a [GenericRecord] for a given data class instance, using
 * reflection to access the fields of the data class.
 *
 * In contrast to [ReflectionRecordEncoder], this encoder requires the class in advance,
 * which it uses to pre-generate some of the reflective calls needed. This approach is faster,
 * but a new encoder must be created for each data class.
 *
 * See [CachedSpecificRecordEncoder].
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

   override fun encode(schema: Schema): (T) -> Any? {
      require(schema.type == Schema.Type.RECORD) { "Provided schema must be a RECORD" }

      val encoders = kclass.declaredMemberProperties.map { member: KProperty1<out Any, *> ->
         val field = schema.getField(member.name) ?: error("Could not find field ${member.name} in schema")
         val encoder = Encoder.encoderFor(member.returnType) as Encoder<Any?>
         Triple(encoder.encode(field.schema()), member.getter, field.pos())
      }

      return { value ->
         val record = GenericData.Record(schema)
         encoders.map { (encode, getter, pos) ->
            val v = getter.call(value)
            val encoded = encode.invoke(v)
            record.put(pos, encoded)
         }
         record
      }
   }
}
