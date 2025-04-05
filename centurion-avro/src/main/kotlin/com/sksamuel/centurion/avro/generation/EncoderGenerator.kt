package com.sksamuel.centurion.avro.generation

import org.apache.avro.Schema

/**
 * Given an Avro schema will generate Kotlin code to create a generic record from a data class
 * that matches that schema.
 */
class EncoderGenerator {

   /**
    * Returns a string representing Kotlin code.
    * Assumptions:
    *
    * - The data class name is the simple name in the schema
    * - The encoder package is the namespace in the schema
    */
   fun generate(schema: Schema): String {
      return generateRecordEncoder(schema)
   }

   private fun generateRecordEncoder(schema: Schema): String {
      require(schema.type == Schema.Type.RECORD)
      val encoderName = schema.name.first().lowercase() + schema.name.drop(1)

      return buildString {
         appendLine("""package ${schema.namespace}""")
         appendLine()
         appendLine("import com.sksamuel.centurion.avro.encoders.*")
         appendLine("import org.apache.avro.generic.GenericData")
         appendLine()
         appendLine("""val ${encoderName}Encoder: Encoder<${schema.name}> = Encoder<${schema.name}> { schema ->""")
         appendLine("""  {""")
         appendLine("""    val record = GenericData.Record(schema)""")
         schema.fields.forEach { field ->
            when (field.schema().type) {
               Schema.Type.RECORD -> TODO()
               Schema.Type.ENUM -> appendLine("""    record.put("${field.name()}", EnumEncoder.encode(it.${field.name()}))""")
               Schema.Type.ARRAY -> appendLine("""    record.put("${field.name()}", LongArrayEncoder.encode(it.${field.name()}))""")
               Schema.Type.MAP -> TODO()
               Schema.Type.UNION -> appendLine("""    record.put("${field.name()}", it.${field.name()})""")
               Schema.Type.FIXED -> TODO()
               Schema.Type.STRING -> appendLine("""    record.put("${field.name()}", StringEncoder.encode(it.${field.name()}))""")
               Schema.Type.BYTES -> TODO()
               Schema.Type.INT -> appendLine("""    record.put("${field.name()}", it.${field.name()})""")
               Schema.Type.LONG -> appendLine("""    record.put("${field.name()}", it.${field.name()})""")
               Schema.Type.FLOAT -> appendLine("""    record.put("${field.name()}", it.${field.name()})""")
               Schema.Type.DOUBLE -> appendLine("""    record.put("${field.name()}", it.${field.name()})""")
               Schema.Type.BOOLEAN -> appendLine("""    record.put("${field.name()}", it.${field.name()})""")
               Schema.Type.NULL -> TODO()
            }
         }
         appendLine("""    record""")
         appendLine("""  }""")
         appendLine("""}""")
      }
   }
}
