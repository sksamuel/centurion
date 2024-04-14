package com.sksamuel.centurion.avro.generation

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties

class ReflectionSchemaBuilder {

   fun schema(kclass: KClass<*>): Schema {

      require(kclass.isData) { "Can only be called on data classes: was $kclass" }

      val builder = SchemaBuilder.record(kclass.java.name).namespace(kclass.java.packageName)
      return kclass.memberProperties.fold(builder.fields()) { acc, op ->

         val typeBuilder = if (op.returnType.isMarkedNullable)
            SchemaBuilder.nullable()
         else
            SchemaBuilder.builder()

         val type = when (val classifier = op.returnType.classifier) {
            String::class -> typeBuilder.stringType()
            Boolean::class -> typeBuilder.booleanType()
            Int::class -> typeBuilder.intType()
            Long::class -> typeBuilder.longType()
            Short::class -> typeBuilder.intType()
            Byte::class -> typeBuilder.intType()
            Double::class -> typeBuilder.doubleType()
            Float::class -> typeBuilder.floatType()
            else -> error("Unsupported type $classifier")
         }

         acc.name(op.name).type(type).noDefault()
      }.endRecord()
   }

}
