package com.sksamuel.centurion.avro.generation

class RecordEncoderGenerator {

   fun generate(ds: DataClass): String {
      return buildString {
         appendLine("package ${ds.packageName}")
         appendLine()
         appendLine("import com.sksamuel.centurion.avro.encoders.*")
         appendLine("import org.apache.avro.Schema")
         appendLine("import org.apache.avro.generic.GenericData")
         appendLine("import org.apache.avro.generic.GenericRecord")
         appendLine()
         appendLine("/**")
         appendLine(" * This is a generated [Encoder] that encodes [${ds.className}]s to Avro [GenericRecord]s")
         appendLine(" */")
         appendLine("object ${ds.className}Encoder : Encoder<${ds.className}> {")
         appendLine("  override fun encode(schema: Schema, value: ${ds.className}): GenericRecord {")
         appendLine("    val record = GenericData.Record(schema)")
         ds.members.forEach { member ->
            appendLine("    record.put(\"${member.name}\", ${encoderFor(member)})")
         }
         appendLine("    return record")
         appendLine("  }")
         appendLine("}")
      }
   }

   private fun encoderFor(member: Member): String {
      val getSchema = "schema.getField(\"${member.name}\").schema()"
      return when (member.type) {
         Type.BooleanType -> "BooleanEncoder.encode($getSchema, value.${member.name})"
         Type.DoubleType -> "DoubleEncoder.encode($getSchema, schema, value.${member.name})"
         Type.FloatType -> "FloatEncoder.encode($getSchema, schema, value.${member.name})"
         Type.IntType -> "IntEncoder.encode($getSchema, value.${member.name})"
         Type.LongType -> "LongEncoder.encode($getSchema, value.${member.name})"
         is Type.Nullable -> TODO()
         is Type.RecordType -> TODO()
         Type.StringType -> "StringEncoder.encode($getSchema, value.${member.name})"
         is Type.ArrayType -> "ListEncoder.encode($getSchema, value.${member.name})"
         is Type.MapType -> "MapEncoder.encode($getSchema, value.${member.name})"
      }
   }
}
