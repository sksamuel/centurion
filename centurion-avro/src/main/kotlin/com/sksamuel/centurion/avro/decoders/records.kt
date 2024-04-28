package com.sksamuel.centurion.avro.decoders

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import kotlin.reflect.KClass
import kotlin.reflect.full.primaryConstructor

class SpecificRecordDecoder<T : Any>(
   private val kclass: KClass<T>,
) : Decoder<T> {

   init {
      require(kclass.isData) { "SpecificRecordDecoder only support data class: was $kclass" }
   }

   companion object {
      inline operator fun <reified T : Any> invoke() = SpecificRecordDecoder(T::class)
   }

   private val constructor = kclass.primaryConstructor ?: error("No primary constructor")

   override fun decode(schema: Schema): (Any?) -> T {
      require(schema.type == Schema.Type.RECORD)

      val members = constructor.parameters.map { param ->
         val field = schema.getField(param.name)
         val decoder = Decoder.decoderFor(param.type)
         Pair(field.pos(), decoder.decode(field.schema()))
      }

      return { record ->
         require(record is GenericRecord) { "SpecificRecordDecoder only supports GenericRecords: was $record" }

         val args = members.map { (pos, decode) ->
            val arg = record.get(pos)
            decode(arg)
         }

         constructor.call(*args.toTypedArray())
      }
   }
}
