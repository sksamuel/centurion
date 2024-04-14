package com.sksamuel.centurion.avro.generation

import org.apache.avro.Schema

/**
 * Generates Kotlin code to programatically create a schema from a given avro schema field.
 */
class SchemaBuilderGenerator {

   fun generate(schema: Schema): String {
      require(schema.type == Schema.Type.RECORD)
      require(schema.namespace != null) { "Must specify a namespace to be used as the package name" }
      return buildString {
         appendLine("package ${schema.namespace}")
         appendLine()
         appendLine("import com.sksamuel.centurion.avro.generation.GenericRecordEncoder")
         appendLine("import org.apache.avro.Schema")
         appendLine("import org.apache.avro.SchemaBuilder")
         appendLine()
         appendLine("val schema${schema.name} = SchemaBuilder.record(\"${schema.name}\").namespace(\"${schema.namespace}\")")
         appendLine(".fields()")
         schema.fields.forEach { field ->
            appendLine(".name(\"${field.name()}\").${type(field.schema(), false, field.name())}")
         }
         appendLine(".endRecord()")
      }
   }

   private fun type(schema: Schema, optional: Boolean, name: String): String {
      return when (schema.type) {
         Schema.Type.RECORD -> TODO()
         Schema.Type.ENUM -> TODO()
         Schema.Type.ARRAY -> TODO()
         Schema.Type.MAP -> TODO()
         Schema.Type.UNION -> if (schema.isNullable) type(schema.types[1], true, name) else error("$schema")
         Schema.Type.FIXED -> TODO()
         Schema.Type.BYTES -> TODO()
         Schema.Type.INT -> if (optional) "type().optional().intType()" else "type().intType().noDefault()"
         Schema.Type.LONG -> if (optional) "type().optional().longType()" else "type().longType().noDefault()"
         Schema.Type.STRING -> if (optional) "type().optional().stringType()" else "type().stringType().noDefault()"
         Schema.Type.FLOAT -> if (optional) "type().optional().floatType()" else "type().floatType().noDefault()"
         Schema.Type.DOUBLE -> if (optional) "type().optional().doubleType()" else "type().doubleType().noDefault()"
         Schema.Type.BOOLEAN -> if (optional) "type().optional().booleanType()" else "type().booleanType().noDefault()"
         Schema.Type.NULL -> TODO()
         else -> error("Unsupported schema $schema")
      }
   }
}
