package com.sksamuel.centurion.avro.generation

/**
 * A code generator that outputs Kotlin that will deserialize a generic record for a given
 * type into an instance of that data class.
 */
class RecordDecoderGenerator {
   fun generate(ds: DataClass): String {
      return buildString {
         appendLine("package ${ds.packageName}")
         appendLine()
         appendLine("import com.sksamuel.centurion.avro.generation.GenericRecordDeserializer")
         appendLine("import org.apache.avro.Schema")
         appendLine("import org.apache.avro.generic.GenericData")
         appendLine("import org.apache.avro.generic.GenericRecord")
         appendLine()
         appendLine("/**")
         appendLine(" * This is a generated [GenericRecordDeserializer] that deserializes Avro [GenericRecord]s to [${ds.className}]s")
         appendLine(" */")
         appendLine("object ${ds.className}Decoder : GenericRecordDecoder<${ds.className}> {")
         appendLine("  override fun decode(record: GenericRecord): ${ds.className} {")
         appendLine("    return ${ds.className}(")
         ds.members.forEach {
            appendLine("      ${it.name} = record.get(\"${it.name}\"),")
         }
         appendLine("    )")
         appendLine("  }")
         appendLine("}")
      }
   }
}
