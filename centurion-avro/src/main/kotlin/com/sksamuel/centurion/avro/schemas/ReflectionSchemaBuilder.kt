package com.sksamuel.centurion.avro.schemas

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.memberProperties

/**
 * Uses reflection to build a [org.apache.avro.Schema] from a [kotlin.reflect.KClass].
 */
class ReflectionSchemaBuilder(
   private val useJavaString: Boolean = false,
) {

   private val utf8String = SchemaBuilder.builder().stringType()
   private val javaString = SchemaBuilder.builder().stringType().also {
      GenericData.setStringType(it, GenericData.StringType.String)
   }

   fun schema(kclass: KClass<*>): Schema {

      require(kclass.isData) { "Can only be called on data classes: was $kclass" }

      val builder = SchemaBuilder.record(kclass.java.name).namespace(kclass.java.packageName)
      return kclass.memberProperties.fold(builder.fields()) { acc, op ->
         acc.name(op.name).type(schemaFor(op.returnType)).noDefault()
      }.endRecord()
   }

   private fun schemaFor(type: KType): Schema {

      val builder = SchemaBuilder.builder()

      val schema: Schema = when (val classifier = type.classifier) {
         String::class -> if (useJavaString) javaString else utf8String
         Boolean::class -> builder.booleanType()
         Int::class -> builder.intType()
         Long::class -> builder.longType()
         Short::class -> builder.intType()
         Byte::class -> builder.intType()
         Double::class -> builder.doubleType()
         Float::class -> builder.floatType()
         Set::class -> builder.array().items(schemaFor(type.arguments.first().type!!))
         List::class -> builder.array().items(schemaFor(type.arguments.first().type!!))
         Array::class -> builder.array().items(schemaFor(type.arguments.first().type!!))
         LongArray::class -> builder.array().items(Schema.create(Schema.Type.LONG))
         IntArray::class -> builder.array().items(Schema.create(Schema.Type.INT))
         Map::class -> builder.map().values(schemaFor(type.arguments[1].type!!))
         is KClass<*> -> if (classifier.java.isEnum)
            builder
               .enumeration(classifier.java.name)
               .namespace(classifier.java.packageName)
               .symbols(*classifier.java.enumConstants.map { (it as Enum<*>).name }
                  .toTypedArray())
         else
            error("Unsupported type $classifier")

         else -> error("Unsupported type $classifier")
      }

      return if (type.isMarkedNullable) SchemaBuilder.unionOf().nullType().and().type(schema).endUnion() else schema
   }
}
