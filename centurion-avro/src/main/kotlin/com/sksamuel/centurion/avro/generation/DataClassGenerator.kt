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
      return Member(field.name(), schema(field.schema()))
   }

   private fun schema(schema: Schema): Type {
      return when (val type = schema.type) {
         Schema.Type.RECORD -> Type.RecordType(schema.namespace, schema.name)
         Schema.Type.STRING -> Type.StringType
         Schema.Type.INT -> Type.IntType
         Schema.Type.LONG -> Type.LongType
         Schema.Type.FLOAT -> Type.FloatType
         Schema.Type.DOUBLE -> Type.DoubleType
         Schema.Type.BOOLEAN -> Type.BooleanType
         Schema.Type.ARRAY -> Type.ArrayType(schema(schema.elementType))
         Schema.Type.MAP -> Type.ArrayType(schema(schema.valueType))
         else -> error("Invalid code path")
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

   data class ArrayType(val elementType: Type) : Type
   data class MapType(val valueType: Type) : Type

   // a nullable type wraps any other type, denoting that it is permitted to be null
   // this is analogous to Avro's union type, with two elements - null and another
   data class Nullable(val element: Type) : Type
}

data class Member(val name: String, val type: Type)
