package com.sksamuel.centurion.avro.generation

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties

class ReflectionSchemaBuilder {

   fun schema(kclass: KClass<*>): Schema {

      require(kclass.isData) { "Can only be called on data classes: was $kclass" }

      val builder = SchemaBuilder.record(kclass.java.name).namespace(kclass.java.packageName)
      return kclass.memberProperties.fold(builder.fields()) { acc, op ->
         acc.name(op.name).type(schemaFor(op.returnType)).noDefault()
      }.endRecord()
   }

   private fun schemaFor(type: KType): Schema {

      val typeBuilder = if (type.isMarkedNullable)
         SchemaBuilder.nullable()
      else
         SchemaBuilder.builder()

      return when (val classifier = type.classifier) {
         String::class -> typeBuilder.stringType()
         Boolean::class -> typeBuilder.booleanType()
         Int::class -> typeBuilder.intType()
         Long::class -> typeBuilder.longType()
         Short::class -> typeBuilder.intType()
         Byte::class -> typeBuilder.intType()
         Double::class -> typeBuilder.doubleType()
         Float::class -> typeBuilder.floatType()
         Set::class -> typeBuilder.array().items(schemaFor(type.arguments.first().type!!))
         List::class -> typeBuilder.array().items(schemaFor(type.arguments.first().type!!))
         is KClass<*> -> if (classifier.java.isEnum)
            typeBuilder
               .enumeration(classifier.java.name)
               .namespace(classifier.java.packageName)
               .symbols(*classifier.java.enumConstants.map { (it as Enum<*>).name }
                  .toTypedArray())
         else
            error("Unsupported type $classifier")

         else -> error("Unsupported type $classifier")
      }
   }
}
