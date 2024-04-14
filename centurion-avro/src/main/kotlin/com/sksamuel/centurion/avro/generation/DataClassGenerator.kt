package com.sksamuel.centurion.avro.generation

import org.apache.avro.Schema

/**
 * Generates a data class model for a given schema.
 */
class DataClassGenerator {

   fun generate(schema: Schema): DataClass {
      require(schema.type == Schema.Type.RECORD) { "Type must be a record in order to generate a data class" }
      val members = schema.fields.map { member(it) }
      return DataClass(schema.namespace, schema.name, members)
   }

   private fun member(field: Schema.Field): Member {
      val type = when (field.schema().type) {
         Schema.Type.RECORD -> Type.RecordType(field.schema().namespace, field.schema().name)
         Schema.Type.STRING -> Type.StringType
         Schema.Type.INT -> Type.IntType
         Schema.Type.LONG -> Type.LongType
         Schema.Type.FLOAT -> Type.FloatType
         Schema.Type.DOUBLE -> Type.DoubleType
         Schema.Type.BOOLEAN -> Type.BooleanType
         else -> error("Invalid code path")
      }
      return Member(field.name(), type)
   }
}

/**
 * Creates a string representation of a data class.
 */
object DataClassWriter {
   fun write(ds: DataClass): String {
      return buildString {
         appendLine("package ${ds.packageName}")
         appendLine()
         appendLine("data class ${ds.className}(")
         ds.members.forEach {
            appendLine("  val ${it.name}: ${typeToString(it.type)},")
         }
         appendLine(")")
      }
   }

   private fun typeToString(type: Type): String {
      return when (type) {
         Type.BooleanType -> "Boolean"
         Type.DoubleType -> "Double"
         Type.FloatType -> "Float"
         Type.IntType -> "Int"
         Type.LongType -> "Long"
         is Type.RecordType -> type.packageName + "." + type.className
         Type.StringType -> "String"
      }
   }
}

/**
 * Models a data class.
 */
data class DataClass(val packageName: String, val className: String, val members: List<Member>)

sealed interface Type {
   data class RecordType(val packageName: String, val className: String) : Type
   object BooleanType : Type
   object StringType : Type
   object IntType : Type
   object LongType : Type
   object FloatType : Type
   object DoubleType : Type
}

data class Member(val name: String, val type: Type)
