package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import java.lang.invoke.LambdaMetafactory
import java.lang.invoke.MethodHandles
import java.lang.invoke.MethodType
import java.lang.reflect.Constructor
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Function
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

class SpecificRecordDecoder<T : Any>(
   private val kclass: KClass<T>,
) : Decoder<T> {

   init {
      require(kclass.isData) { "SpecificRecordDecoder only support data classes: was $kclass" }
   }

   companion object {
      inline operator fun <reified T : Any> invoke() = SpecificRecordDecoder(T::class)
   }

   private val constructor = kclass.primaryConstructor ?: error("No primary constructor")
   private val decoders = ConcurrentHashMap<String, (GenericRecord) -> T>()

   override fun decode(schema: Schema, value: Any?): T {
      val record = value as GenericRecord
      val decodeFn = decoders.getOrPut(value::class.java.name) { decoderFn(schema) }
      return decodeFn(record)
   }

   private fun decoderFn(schema: Schema): (GenericRecord) -> T {
      val lookup = MethodHandles.lookup()

      val mt = MethodType.methodType(Void.TYPE, kclass.java.constructors.first().parameterTypes)
      val handle = lookup.findConstructor(kclass.java, mt)

//      // this is the interface we're going to be implementing with the interface method type
//      val factoryType = MethodType.methodType(Function::class.java)
//
//      // this is the method we will be implementing
//      val interfaceMethodType = MethodType.methodType(kclass.java, GenericRecord::class.java)
//
//      val callSite = LambdaMetafactory.metafactory(
//         /* caller = */ lookup,
//         /* interfaceMethodName = */ "apply", // the name of the method inside the interface
//         /* factoryType = */ factoryType,
//         /* interfaceMethodType = */ MethodType.methodType(Any::class.java, Any::class.java), // erased apply
//         /* implementation = */ handle, // this is the reflection call that will be inlined
//         /* dynamicMethodType = */ interfaceMethodType, // runtime version of apply
//      )

      val decoders = constructor.parameters.map { param ->
         val avroField = schema.getField(param.name)
         val decoder = Decoder.decoderFor(param.type)
         Decoding(avroField.pos(), decoder, avroField.schema())
      }

      return { record ->
         val args = decoders.map { (pos, decoder, schema) ->
            val value = record.get(pos)
            decoder.decode(schema, value)
         }
//         handle.invokeWithArguments(args) as T
         constructor.call(*args.toTypedArray())
      }
   }

   private data class Decoding(val pos: Int, val decode: Decoder<*>, val schema: Schema)
}
