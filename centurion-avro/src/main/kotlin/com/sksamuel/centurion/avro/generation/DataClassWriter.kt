package com.sksamuel.centurion.avro.generation

/**
 * Creates a string representation of a data class.
 */
object DataClassWriter {
   fun write(ds: DataClass): String {
      return buildString {
         appendLine("package ${ds.packageName}")
         appendLine()
         appendLine("data class ${ds.className}(")
         ds.members.forEach {
            appendLine("  val ${it.name}: ${typeToString(it.type)},")
         }
         appendLine(")")
      }
   }

   private fun typeToString(type: Type): String {
      return when (type) {
         Type.BooleanType -> "Boolean"
         Type.DoubleType -> "Double"
         Type.FloatType -> "Float"
         Type.IntType -> "Int"
         Type.LongType -> "Long"
         is Type.RecordType -> type.packageName + "." + type.className
         Type.StringType -> "String"
         is Type.Nullable -> typeToString(type.element) + "?"
      }
   }
}
