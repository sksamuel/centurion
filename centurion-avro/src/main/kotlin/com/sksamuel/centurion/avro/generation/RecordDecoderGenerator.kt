package com.sksamuel.centurion.avro.generation

import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.declaredMemberProperties

/**
 * A code generator that outputs Kotlin that will deserialize a generic record for a given
 * type into an instance of that data class.
 */
class RecordDecoderGenerator {

   fun generate(kclass: KClass<*>): String {
      return buildString {
         appendLine("package ${kclass.java.packageName}")
         appendLine()
         appendLine("import com.sksamuel.centurion.avro.decoders.*")
         appendLine("import org.apache.avro.Schema")
         appendLine("import org.apache.avro.generic.GenericData")
         appendLine("import org.apache.avro.generic.GenericRecord")
         appendLine()
         appendLine("/**")
         appendLine(" * This is a generated [Decoder] that deserializes Avro [GenericRecord]s to [${kclass.java.simpleName}]s")
         appendLine(" */")
         appendLine("object ${kclass.java.simpleName}Decoder : Decoder<${kclass.java.simpleName}> {")
         appendLine()
         appendLine("  override fun decode(schema: Schema): (Any?) -> ${kclass.java.simpleName} {")
         appendLine()
         kclass.declaredMemberProperties.forEach { property ->
            appendLine("    val ${property.name}Schema = schema.getField(\"${property.name}\").schema()")
            appendLine("    val ${property.name}Pos    = schema.getField(\"${property.name}\").pos()")
            appendLine("    val ${property.name}Decode = ${decode(property)}")
         }
         appendLine()
         appendLine("    return { record ->")
         appendLine("      require(record is GenericRecord)")
         appendLine("      ${kclass.java.simpleName}(")
         kclass.declaredMemberProperties.forEach { property ->
            appendLine("        ${property.name} = ${property.name}Decode(record[${property.name}Pos]),")
         }
         appendLine("      )")
         appendLine("    }")
         appendLine("  }")
         appendLine("}")
      }
   }

   private fun decode(property: KProperty1<out Any, *>): String {
      val baseDecoder = decoderFor(property.returnType)
      val wrapped = if (property.returnType.isMarkedNullable) "NullDecoder($baseDecoder)" else baseDecoder
      return "$wrapped.decode(${property.name}Schema)"
   }

   private fun decoderFor(type: KType): String {
      return when (val classifier = type.classifier) {
         Boolean::class -> "BooleanDecoder"
         Double::class -> "DoubleDecoder"
         Float::class -> "FloatDecoder"
         Int::class -> "IntDecoder"
         Long::class -> "LongDecoder"
         String::class -> "StringDecoder"
         Set::class -> {
            val elementDecoder = decoderFor(type.arguments.first().type!!)
            "SetDecoder($elementDecoder)"
         }

         List::class -> {
            val elementDecoder = decoderFor(type.arguments.first().type!!)
            "ListDecoder($elementDecoder)"
         }

         Map::class -> {
            val valueDecoder = decoderFor(type.arguments[1].type!!)
            "MapDecoder($valueDecoder)"
         }

         is KClass<*> -> if (classifier.java.isEnum) "EnumDecoder<${classifier.java.name}>()" else error("Unsupported type: $type")
         else -> error("Unsupported type: $type")
      }
   }
}
