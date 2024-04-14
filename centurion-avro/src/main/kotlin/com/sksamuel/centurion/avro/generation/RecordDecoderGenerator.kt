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
         appendLine("import com.sksamuel.centurion.avro.decoders.*")
         appendLine("import org.apache.avro.Schema")
         appendLine("import org.apache.avro.generic.GenericData")
         appendLine("import org.apache.avro.generic.GenericRecord")
         appendLine()
         appendLine("/**")
         appendLine(" * This is a generated [Decoder] that deserializes Avro [GenericRecord]s to [${ds.className}]s")
         appendLine(" */")
         appendLine("object ${ds.className}Decoder : Decoder<${ds.className}> {")
         appendLine("  override fun decode(schema: Schema, value: Any?): ${ds.className} {")
         appendLine("    require(value is GenericRecord)")
         appendLine("    return ${ds.className}(")
         ds.members.forEach { member ->
            appendLine("      ${member.name} = ${decoderFor(member)},")
         }
         appendLine("    )")
         appendLine("  }")
         appendLine("}")
      }
   }


   private fun decoderFor(member: Member): String {
      val getSchema = "schema.getField(\"${member.name}\").schema()"
      val getValue = "value.get(\"${member.name}\")"
      return when (member.type) {
         Type.BooleanType -> "BooleanDecoder.decode($getSchema, $getValue)"
         Type.DoubleType -> "DoubleDecoder.decode($getSchema, $getValue)"
         Type.FloatType -> "FloatDecoder.decode($getSchema, $getValue)"
         Type.IntType -> "IntDecoder.decode($getSchema, $getValue)"
         Type.LongType -> "LongDecoder.decode($getSchema, $getValue)"
         is Type.Nullable -> TODO()
         is Type.RecordType -> TODO()
         Type.StringType -> "StringDecoder.decode($getSchema, $getValue)"
      }
   }
}
