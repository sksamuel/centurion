package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
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
 * The [ReflectionRecordEncoder] will build and cache the reflection calls once the encoder is created
 * from the given schema.
 *
 * This encoder requires a small overhead in CPU time verus programmatically generated encoders of around 10%.
 *
 * To benefit from the cached encodings ensure that you create a reflection-based encoder
 * once per class and re-use it throughout your project.
 *
 * Instances of this class are thread-safe.
 */
class ReflectionRecordEncoder<T : Any>(schema: Schema, kclass: KClass<T>) : Encoder<T> {

   companion object {

      /**
       * Creates a [ReflectionRecordEncoder] for the given [schema] and type [T].
       */
      inline operator fun <reified T : Any> invoke(schema: Schema): ReflectionRecordEncoder<T> {
         return ReflectionRecordEncoder(schema, T::class)
      }

      /**
       * Creates a [ReflectionRecordEncoder] for the given type [T].
       * This will automatically build the Avro schema for the type [T] using
       * [ReflectionSchemaBuilder].
       */
      inline operator fun <reified T : Any> invoke(): ReflectionRecordEncoder<T> {
         val schema = ReflectionSchemaBuilder().schema(T::class)
         return ReflectionRecordEncoder(schema, T::class)
      }

      /**
       * Creates a [ReflectionRecordEncoder] for the given type [T].
       * This will automatically build the Avro schema for the type [T] using
       * [ReflectionSchemaBuilder].
       */
      operator fun <T : Any> invoke(kclass: KClass<T>): ReflectionRecordEncoder<T> {
         val schema = ReflectionSchemaBuilder().schema(kclass)
         return ReflectionRecordEncoder(schema, kclass)
      }
   }

   private val encoders: List<Encoding> = buildEncodings(schema, kclass)

   override fun encode(schema: Schema, value: T): Any? {
      val record = GenericData.Record(schema)
      encoders.map { (encoder, getter, pos, schema) ->
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
         val encoder = Encoder.encoderFor(
            type = member.returnType,
            stringType = schema.getProp(GenericData.STRING_PROP),
            schema = avroField.schema()
         ) as Encoder<Any?>

         // this is the interface we're going to be implementing with the interface method type
         val factoryType = MethodType.methodType(Function::class.java)

         // this is the method we will be implementing
         val interfaceMethodType = MethodType.methodType(getter.returnType, kclass.java)

         val callSite = LambdaMetafactory.metafactory(
            /* caller = */ lookup,
            /* interfaceMethodName = */ "apply", // the name of the SAM inside the Function interface
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


