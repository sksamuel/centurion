package com.sksamuel.centurion.avro.generation

/**
 * Creates a string version of an [Encoder] that can be used to convert this data class
 * to a Record
 */
class GenericRecordDecoderGenerator {
   fun generate(ds: DataClass): String {
      return buildString {
         appendLine("package ${ds.packageName}")
         appendLine()
         appendLine("import com.sksamuel.centurion.avro.generation.GenericRecordDecoder")
         appendLine("import org.apache.avro.Schema")
         appendLine("import org.apache.avro.generic.GenericData")
         appendLine("import org.apache.avro.generic.GenericRecord")
         appendLine()
         appendLine("/**")
         appendLine(" * This is a generated [GenericRecordDecoder] that decodes Avro [GenericRecord]s to [${ds.className}]s")
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
