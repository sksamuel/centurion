package com.sksamuel.centurion.avro.generation

import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.declaredMemberProperties

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
         appendLine("class ${kclass.java.simpleName}Encoder(schema: Schema) : Encoder<${kclass.java.simpleName}> {")
         appendLine()
         kclass.declaredMemberProperties.forEach { property ->
            appendLine("  private val ${property.name}Encoder = ${encoderVal(property)}")
            appendLine("  private val ${property.name}Schema  = schema.getField(\"${property.name}\").schema()")
            appendLine("  private val ${property.name}Pos     = schema.getField(\"${property.name}\").pos()")
         }
         appendLine()
         appendLine("  override fun encode(schema: Schema, value: ${kclass.java.simpleName}): GenericRecord {")
         appendLine("    val record = GenericData.Record(schema)")
         kclass.declaredMemberProperties.forEach { property ->
            appendLine("    record.put(${property.name}Pos, ${encoderInvocation(property)})")
         }
         appendLine("    return record")
         appendLine("  }")
         appendLine("}")
      }
   }

   private fun encoderVal(property: KProperty1<out Any, *>): String {
      val baseEncoder = encoderFor(property.returnType)
      return if (property.returnType.isMarkedNullable) "NullEncoder($baseEncoder)" else baseEncoder
   }

   private fun encoderInvocation(property: KProperty1<out Any, *>): String {
      val getSchema = "${property.name}Schema"
      val getValue = "value.${property.name}"
      return "${property.name}Encoder.encode($getSchema, $getValue)"
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
