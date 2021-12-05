package com.sksamuel.centurion.orc

import com.sksamuel.centurion.Schema
import org.apache.orc.TypeDescription

object Schemas {

  fun toOrc(schema: Schema): TypeDescription {
    return when (schema) {
      is Schema.Array -> TypeDescription.createList(toOrc(schema.elements))
      is Schema.Map -> TypeDescription.createMap(TypeDescription.createString(), toOrc(schema.values))
      is Schema.Struct -> schema.fields.fold(TypeDescription.createStruct()) { acc, field ->
        acc.addField(field.name, toOrc(field.schema))
      }
      Schema.Booleans -> TypeDescription.createBoolean()
      Schema.Bytes -> TypeDescription.createBinary()
      Schema.Int8 -> TypeDescription.createByte()
      Schema.Int16 -> TypeDescription.createShort()
      Schema.Int32 -> TypeDescription.createInt()
      Schema.Int64 -> TypeDescription.createLong()
      Schema.Float64 -> TypeDescription.createDouble()
      Schema.Float32 -> TypeDescription.createFloat()
      Schema.Strings -> TypeDescription.createString()
      Schema.TimestampMillis -> TypeDescription.createTimestamp()
      is Schema.Varchar -> TypeDescription.createVarchar().withMaxLength(schema.length)
      else -> error("Unsupported schema $schema")
    }
  }

  fun fromOrc(type: TypeDescription): Schema {
    return when (type.category!!) {
      TypeDescription.Category.BINARY -> Schema.Bytes
      TypeDescription.Category.BOOLEAN -> Schema.Booleans
      TypeDescription.Category.DOUBLE -> Schema.Float64
      TypeDescription.Category.FLOAT -> Schema.Float32
      TypeDescription.Category.INT -> Schema.Int32
      TypeDescription.Category.LIST -> Schema.Array(fromOrc(type.children[0]))
      TypeDescription.Category.LONG -> Schema.Int64
      TypeDescription.Category.MAP -> Schema.Map(fromOrc(type.children[1]))
      TypeDescription.Category.STRING -> Schema.Strings
      TypeDescription.Category.STRUCT -> {
        val fields = type.fieldNames.zip(type.children).map { (name, child) ->
          Schema.Field(name, fromOrc(child))
        }
        Schema.Struct("struct", fields)
      }
      TypeDescription.Category.VARCHAR -> Schema.Varchar(type.maxLength)
      TypeDescription.Category.BYTE -> Schema.Int8
      TypeDescription.Category.SHORT -> Schema.Int16
      TypeDescription.Category.TIMESTAMP -> Schema.TimestampMillis
      else -> error("Unsupported type $type")
    }
  }
}
