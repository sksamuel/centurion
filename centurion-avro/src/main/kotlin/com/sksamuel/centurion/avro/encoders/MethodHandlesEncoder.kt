package com.sksamuel.centurion.avro.encoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import java.lang.invoke.LambdaMetafactory
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

class Foo {
   private val field_a: String = "a"
}

data class Bar(val field_a: String)

fun main() {

   val lookup = MethodHandles.privateLookupIn(Bar::class.java, MethodHandles.lookup())

   val field = Bar::class.java.getDeclaredField("field_a")
   field.trySetAccessible()

   val method = Bar::class.java.getDeclaredMethod("getField_a")

//   val getter = lookup.findGetter(Bar::class.java, "field_a", String::class.java)
   val methodHandle = lookup.unreflect(method)

   // this is the interface we're going to be implementing with the interface method type
   val factoryType = MethodType.methodType(java.util.function.Function::class.java)

   // this is the method we will be implementing, it'll return the field type and accept an instance of Bar
   val interfaceMethodType = MethodType.methodType(String::class.java, Bar::class.java)

   val callSite = LambdaMetafactory.metafactory(
      /* caller = */ lookup,
      /* interfaceMethodName = */ "apply", // the name of the method inside the interface
      /* factoryType = */ factoryType,
      /* interfaceMethodType = */ MethodType.methodType(Any::class.java, Any::class.java),
      /* implementation = */ methodHandle, // this is the reflection call that will be inlined
      /* dynamicMethodType = */ interfaceMethodType
   )

   val fn = callSite.target.invoke() as java.util.function.Function<Bar, String>

//   println(getter.invoke(Bar("qwe")))
   println(methodHandle.invoke(Bar("bdf")))
   println(fn.apply(Bar("dfg")))

}

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

   override fun encode(schema: Schema): (T) -> Any? {
      require(schema.type == Schema.Type.RECORD) { "Provided schema must be a RECORD" }

      val lookup = MethodHandles.lookup()

      val encoders = kclass.declaredMemberProperties.map { member: KProperty1<out Any, *> ->

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
            /* interfaceMethodType = */ MethodType.methodType(Any::class.java, Any::class.java), // erased aply
            /* implementation = */ methodHandle, // this is the reflection call that will be inlined
            /* dynamicMethodType = */ interfaceMethodType, // runtime version of apply
         )

         val fn = callSite.target.invoke() as java.util.function.Function<T, Any?>

         Triple(encoder.encode(avroField.schema()), fn, avroField.pos())
      }

      return { value: T ->
         val record = GenericData.Record(schema)
         encoders.map { (encode, getter, pos) ->
            val v = getter.apply(value)
            val encoded = encode.invoke(v)
            record.put(pos, encoded)
         }
         record
      }
   }
}

