package com.sksamuel.centurion.avro.generation

import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.starProjectedType

/**
 * Generates Kotlin code to programatically create an Encoder for a specific type.
 */
class RecordEncoderGenerator {

   fun generate(kclass: KClass<*>): String {
      require(kclass.isData) { "Generator supports data classes only: was $kclass" }
      return buildString {
         appendLine("package ${kclass.java.packageName}")
         appendLine()
         appendLine("import com.sksamuel.centurion.avro.encoders.*")
         appendLine("import org.apache.avro.Schema")
         appendLine("import org.apache.avro.generic.GenericData")
         appendLine("import org.apache.avro.generic.GenericRecord")
         appendLine()
         appendLine("/**")
         appendLine(" * This is a generated [Encoder] that encodes [${kclass.java.simpleName}]s to Avro [GenericRecord]s")
         appendLine(" */")
         appendLine("object ${kclass.java.simpleName}Encoder : Encoder<${kclass.java.simpleName}> {")
         appendLine("  override fun encode(schema: Schema, value: ${kclass.java.simpleName}): GenericRecord {")
         appendLine("    val record = GenericData.Record(schema)")
         kclass.declaredMemberProperties.forEach { property ->
            appendLine("    record.put(\"${property.name}\", ${encoderFor(property)})")
         }
         appendLine("    return record")
         appendLine("  }")
         appendLine("}")
      }
   }

   private fun encoderFor(property: KProperty1<out Any, *>): String {
      val getSchema = "schema.getField(\"${property.name}\").schema()"
      val getValue = "value.${property.name}"
      val encoder = encoderFor(property.returnType)
      return "$encoder.encode($getSchema, $getValue)"
   }

   private fun encoderFor(type: KType): String {
      return when (val classifier = type.classifier) {
         Boolean::class -> "BooleanEncoder"
         Double::class -> "DoubleEncoder"
         Float::class -> "FloatEncoder"
         Int::class -> "IntEncoder"
         Long::class -> "LongEncoder"
         String::class -> "StringEncoder"
         Set::class -> {
            val elementEncoder = encoderFor(type.arguments.first().type!!)
            "SetEncoder($elementEncoder)"
         }

         List::class -> {
            val elementEncoder = encoderFor(type.arguments.first().type!!)
            "ListEncoder($elementEncoder)"
         }

         Map::class -> {
            val valueEncoder = encoderFor(type.arguments[1].type!!)
            "MapEncoder(StringEncoder, $valueEncoder)"
         }

         is KClass<*> -> if (classifier.java.isEnum) "EnumEncoder()" else error("Unsupported type: $type")
         else -> error("Unsupported type: $type")
      }
   }
}
