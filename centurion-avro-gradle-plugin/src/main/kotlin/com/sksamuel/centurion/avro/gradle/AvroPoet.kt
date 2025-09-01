package com.sksamuel.centurion.avro.gradle

import com.squareup.kotlinpoet.AnnotationSpec
import com.squareup.kotlinpoet.ClassName
import com.squareup.kotlinpoet.CodeBlock
import com.squareup.kotlinpoet.FileSpec
import com.squareup.kotlinpoet.FunSpec
import com.squareup.kotlinpoet.KModifier
import com.squareup.kotlinpoet.ParameterSpec
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import com.squareup.kotlinpoet.PropertySpec
import com.squareup.kotlinpoet.TypeName
import com.squareup.kotlinpoet.TypeSpec
import com.squareup.kotlinpoet.asClassName
import com.squareup.kotlinpoet.asTypeName
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import java.nio.file.Path
import java.sql.Timestamp

class AvroPoet {

   private val types = mutableListOf<TypeSpec>()
   private val parser = Schema.Parser()

   fun generate(input: Path) {

      val schema = parser.parse(input.toFile())
      record(schema)

      val spec = FileSpec.builder(schema.namespace, schema.name)
      spec.addImport(GenericData::class.java.`package`.name, "GenericData")
      spec.addImport(Utf8::class.java.`package`.name, "Utf8")

      types.distinctBy { it.name }.forEach { spec.addType(it) }
   }

   private fun ref(schema: Schema): TypeName {
      return when (schema.type) {
         Schema.Type.RECORD -> ClassName(schema.namespace, schema.name)
         Schema.Type.ENUM -> enum(schema)
         Schema.Type.ARRAY -> ClassName("kotlin.collections", "List").parameterizedBy(ref(schema.elementType))
         Schema.Type.MAP -> ClassName("kotlin.collections", "Map").parameterizedBy(
            String::class.asClassName(),
            ref(schema.valueType)
         )

         Schema.Type.UNION -> union(schema)
         Schema.Type.FIXED -> TODO("FIXED type is not supported, use BYTES")
         Schema.Type.STRING -> String::class.asTypeName()
         Schema.Type.BYTES -> ByteArray::class.asTypeName()
         Schema.Type.INT -> Int::class.asTypeName()
         Schema.Type.LONG -> schemaForLong(schema)
         Schema.Type.FLOAT -> Float::class.asTypeName()
         Schema.Type.DOUBLE -> Double::class.asTypeName()
         Schema.Type.BOOLEAN -> Boolean::class.asTypeName()
         Schema.Type.NULL -> TODO("NULL is not supported outside of a union")
         null -> error("Invalid code path")
      }
   }

   private fun schemaForLong(schema: Schema): TypeName {
      return when (schema.logicalType) {
         is LogicalTypes.TimestampMillis -> Timestamp::class.asTypeName()
         else -> Long::class.asTypeName()
      }
   }

   private fun enum(schema: Schema): ClassName {
      val builder = TypeSpec.enumBuilder(schema.name)
      schema.enumSymbols.forEach { builder.addEnumConstant(it) }
      builder.build().apply { types.add(this) }
      return ClassName(schema.namespace, schema.name)
   }

   private fun union(schema: Schema): TypeName {
      require(schema.isUnion)
      require(schema.types.size == 2)

      require(schema.types.first().type == Schema.Type.NULL) { "Only unions of <null, A> are supported" }
      require(schema.types[1].type != Schema.Type.NULL) { "Only unions of <null, A> are supported" }

      return ref(schema.types[1]).copy(nullable = true)
   }

   private fun record(schema: Schema): ClassName {
      require(schema.type == Schema.Type.RECORD) { "$schema must be record" }

      val type = TypeSpec.classBuilder(schema.name).addModifiers(KModifier.DATA)
      val isDeprecated = schema.getObjectProp("deprecated")?.toString().toBoolean()
      val constructor = FunSpec.constructorBuilder()

      if (isDeprecated) {

         type.addAnnotation(
            AnnotationSpec.builder(Deprecated::class)
               .addMember("\"Deprecated\"")
               .build()
         )

         constructor.addAnnotation(
            AnnotationSpec.builder(Deprecated::class)
               .addMember("\"Deprecated\"")
               .build()
         )
      }

      schema.fields.map { field ->

         val ref = ref(field.schema())

         val param = ParameterSpec.builder(field.name(), ref)
         constructor.addParameter(param.build())

         val prop = PropertySpec.builder(field.name(), ref).initializer(field.name())
         type.addProperty(prop.build())
      }

      val ref = ClassName(schema.namespace, schema.name)

      val decoder = FunSpec.builder("decode")
         .addModifiers(KModifier.OVERRIDE)
         .addParameter("record", GenericRecord::class.asClassName())
         .returns(ref)

      val decoderBody = CodeBlock.builder()

      schema.fields.forEach {
         if (it.schema().isNullable) {
            decoderBody.addStatement(
               "val ${it.name()} = if (record.hasField(%S)) record.get(%S) else null",
               it.name(),
               it.name()
            )
         } else {
            decoderBody.addStatement("val ${it.name()} = record.get(%S)", it.name())
         }
      }

      decoderBody.addStatement("")

      decoderBody.addStatement("return ${schema.name}(").indent()
      schema.fields.forEach {
//         decoderBody.add(decode(it.schema(), it.name()).toBuilder().add(",\n").build())
      }
      decoderBody.unindent()
      decoderBody.addStatement(")")

      decoder.addCode(decoderBody.build())

      val schemaInit = CodeBlock.builder()
         .addStatement(
            "Schema.Parser()" +
               ".parse(this::class.java.getResourceAsStream(%S))",
            "/" + schema.fullName + ".avsc",
         ).indent()

      val schemaFn = PropertySpec.builder("schema", Schema::class, KModifier.OVERRIDE).initializer(schemaInit.build())

      val companion = TypeSpec.companionObjectBuilder()
         .addFunction(decoder.build())
         .addProperty(schemaFn.build())
         .build()

      val encoder = FunSpec.builder("encode")
         .returns(GenericRecord::class.asClassName())
         .addModifiers(KModifier.OVERRIDE)
         .addStatement("val schema = ${schema.name}.schema")
         .addStatement("val record = GenericData.Record(schema)")

      schema.fields.forEach {
//         encoder.addStatement("record.put(%S, ${encode(it.schema(), it.name())})", it.name())
      }

      encoder.addStatement("return record")

      type
         .primaryConstructor(constructor.build())
         .addType(companion)
         .addFunction(encoder.build())
         .build()
         .apply { types.add(this) }

      return ref
   }
}

