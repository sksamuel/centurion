@file:Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")

package com.sksamuel.centurion.avro

import com.sksamuel.centurion.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.Schema as AvroSchema

object Schemas {

  fun toAvro(schema: Schema): AvroSchema {
    return when (schema) {
      is Schema.Strings -> SchemaBuilder.builder().stringType()
      is Schema.Booleans -> SchemaBuilder.builder().booleanType()
      is Schema.Bytes -> SchemaBuilder.builder().bytesType()
      is Schema.Int32 -> SchemaBuilder.builder().intType()
      is Schema.Int64 -> SchemaBuilder.builder().longType()
      is Schema.Array -> SchemaBuilder.builder().array().items(toAvro(schema.elements))
      else -> error("Unsupported schema $schema")
    }
  }

  fun fromAvro(schema: AvroSchema): Schema {
    return when (schema.type) {
      org.apache.avro.Schema.Type.RECORD -> TODO()
      org.apache.avro.Schema.Type.ENUM -> TODO()
      org.apache.avro.Schema.Type.ARRAY -> Schema.Array(fromAvro(schema.elementType))
      org.apache.avro.Schema.Type.MAP -> TODO()
      org.apache.avro.Schema.Type.UNION -> TODO()
      org.apache.avro.Schema.Type.FIXED -> TODO()
      org.apache.avro.Schema.Type.STRING -> Schema.Strings
      org.apache.avro.Schema.Type.BYTES -> Schema.Bytes
      org.apache.avro.Schema.Type.INT -> Schema.Int32
      org.apache.avro.Schema.Type.LONG -> Schema.Int64
      org.apache.avro.Schema.Type.FLOAT -> TODO()
      org.apache.avro.Schema.Type.DOUBLE -> TODO()
      org.apache.avro.Schema.Type.BOOLEAN -> Schema.Booleans
      org.apache.avro.Schema.Type.NULL -> TODO()
    }
  }
}
