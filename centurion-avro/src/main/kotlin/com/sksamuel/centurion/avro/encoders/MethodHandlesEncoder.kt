package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.lang.invoke.LambdaMetafactory
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

/**
 * An [Encoder] that returns a [org.apache.avro.generic.GenericRecord] for a given data class instance, using
 * reflection to access the fields of the data class.
 *
 * In contrast to [ReflectionRecordEncoder], this encoder requires the class in advance,
 * which it uses to pre-generate some of the reflective calls needed. This approach is faster,
 * but a new encoder must be created for each data class.
 *
 * See [CachedSpecificRecordEncoder].
 */
class MethodHandlesEncoder<T : Any>(
   private val kclass: KClass<T>,
) : Encoder<T> {

   companion object {
      inline operator fun <reified T : Any> invoke(): MethodHandlesEncoder<T> = MethodHandlesEncoder(T::class)
   }

   init {
      require(kclass.isData) { "Can only encode data classes: $kclass" }
   }

   private val encoders = ConcurrentHashMap<Int, List<Params>>()

   private fun generateEncoders(schema: Schema): List<Params> {
      require(schema.type == Schema.Type.RECORD) { "Provided schema must be a RECORD" }

      val lookup = MethodHandles.lookup()

      return kclass.declaredMemberProperties.map { member: KProperty1<out Any, *> ->

         val avroField = schema.getField(member.name) ?: error("Could not find field ${member.name} in schema")

         val methodName = "get${member.name.replaceFirstChar { it.uppercase() }}"
         val getter = kclass.java.getDeclaredMethod(methodName) ?: error("Could not find method for ${member.name}")

         val methodHandle = lookup.unreflect(getter)
         val encoder = Encoder.encoderFor(member.returnType) as Encoder<Any?>

         // this is the interface we're going to be implementing with the interface method type
         val factoryType = MethodType.methodType(java.util.function.Function::class.java)

         // this is the method we will be implementing
         val interfaceMethodType = MethodType.methodType(getter.returnType, kclass.java)

         val callSite = LambdaMetafactory.metafactory(
            /* caller = */ lookup,
            /* interfaceMethodName = */ "apply", // the name of the method inside the interface
            /* factoryType = */ factoryType,
            /* interfaceMethodType = */ MethodType.methodType(Any::class.java, Any::class.java), // erased apply
            /* implementation = */ methodHandle, // this is the reflection call that will be inlined
            /* dynamicMethodType = */ interfaceMethodType, // runtime version of apply
         )

         val fn = callSite.target.invokeExact() as java.util.function.Function<Any?, Any?>

         Params(encoder, fn, avroField.pos(), avroField.schema())
      }
   }

   override fun encode(schema: Schema, value: T): Any? {
      val encoders = encoders.getOrPut(0) { generateEncoders(schema) }
      val record = GenericData.Record(schema)
      encoders.map { (encode, getter, pos, schema) ->
         val value = getter.apply(value)
         val encoded = encode.encode(schema, value)
         record.put(pos, encoded)
      }
      return record
   }
}

private data class Params(val encoder: Encoder<Any?>, val fn: Function<Any?, Any?>, val pos: Int, val schema: Schema)
