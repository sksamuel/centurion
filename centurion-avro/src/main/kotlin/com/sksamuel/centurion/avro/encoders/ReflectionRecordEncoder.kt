package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.lang.invoke.LambdaMetafactory
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.util.function.Function
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

/**
 * An [Encoder] that returns a [org.apache.avro.generic.GenericRecord] for data classes, using
 * reflection to access the fields of the data class.
 *
 * The [ReflectionRecordEncoder] will cache the reflection calls for each data class upon first use.
 * This encoder requires a small overhead in CPU time to build the reflection calls,
 * verus programmatically generated encoders of around 10-15%. To benefit from the cached encodings,
 * ensure that you create a reflection based encoder once and re-use it throughout your project.
 *
 * Instances of this class are thread safe.
 */
class ReflectionRecordEncoder<T : Any> : Encoder<T> {

   // this isn't thread safe, but worst case is we generate the same encoders more than once
   // in which case we will have a tiny performance hit initially, but the idea is this class is
   // created once and re-used throughout the service's lifetime
   private var encoders: List<Encoding>? = null

   override fun encode(schema: Schema, value: T): Any? {

      if (encoders == null)
         encoders = buildEncodings(schema, value::class)

      val record = GenericData.Record(schema)
      encoders!!.map { (encoder, getter, pos, schema) ->
         val value = getter.apply(value)
         val encoded = encoder.encode(schema, value)
         record.put(pos, encoded)
      }
      return record
   }

   @Suppress("UNCHECKED_CAST")
   private fun buildEncodings(schema: Schema, kclass: KClass<out Any>): List<Encoding> {
      val lookup = MethodHandles.lookup()
      return kclass.declaredMemberProperties.map { member: KProperty1<out Any, *> ->

         val avroField = schema.getField(member.name)
            ?: error("Could not find field ${member.name} in Avro schema")

         val getter = kclass.java.getDeclaredMethod(fieldGetterName(member.name))
            ?: error("Could not find Java getter method for ${member.name}")

         val methodHandle = lookup.unreflect(getter)
         val encoder = Encoder.encoderFor(member.returnType, schema.getProp(GenericData.STRING_PROP)) as Encoder<Any?>

         // this is the interface we're going to be implementing with the interface method type
         val factoryType = MethodType.methodType(Function::class.java)

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

         val fn = callSite.target.invokeExact() as Function<Any?, Any?>

         Encoding(encoder, fn, avroField.pos(), avroField.schema())
      }
   }

   private fun fieldGetterName(name: String) = "get${name.replaceFirstChar { it.uppercase() }}"

   private data class Encoding(
      val encoder: Encoder<Any?>,
      val fn: Function<Any?, Any?>,
      val pos: Int,
      val schema: Schema
   )
}


