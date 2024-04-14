package com.sksamuel.centurion.avro.generation

/**
 * Creates a string version of an [Encoder] that can be used to convert this data class
 * to a Record
 */
class GenericRecordEncoderGenerator {
   fun generate(ds: DataClass): String {
      return buildString {
         appendLine("package ${ds.packageName}")
         appendLine()
         appendLine("import com.sksamuel.centurion.avro.GenericRecordEncoder")
         appendLine("import org.apache.avro.Schema")
         appendLine("import org.apache.avro.generic.GenericData")
         appendLine("import org.apache.avro.generic.GenericRecord")
         appendLine()
         appendLine("/**")
         appendLine(" * This is a generated [GenericRecordEncoder] that encodes [${ds.className}]s to Avro [GenericRecord]s")
         appendLine(" */")
         appendLine("object ${ds.className}Encoder : GenericRecordEncoder<${ds.className}> {")
         appendLine("  private val schema = Schema.create(Schema.Type.STRING)")
         appendLine("  override fun encode(value: ${ds.className}): GenericRecord {")
         appendLine("    val record = GenericData.Record(schema)")
         ds.members.forEach {
            appendLine("    record.put(\"${it.name}\", value.${it.name})")
         }
         appendLine("    return record")
         appendLine("  }")
         appendLine("}")
      }
   }
}
