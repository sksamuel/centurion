@file:Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")

package com.sksamuel.centurion.avro

import com.sksamuel.centurion.Schema
import org.apache.avro.LogicalTypes
import org.apache.avro.SchemaBuilder
import org.apache.avro.Schema as AvroSchema

object Schemas {

  fun toAvro(schema: Schema): AvroSchema {
    return when (schema) {
      is Schema.Strings -> SchemaBuilder.builder().stringType()
      is Schema.Booleans -> SchemaBuilder.builder().booleanType()
      is Schema.Bytes -> SchemaBuilder.builder().bytesType()
      is Schema.Int64 -> SchemaBuilder.builder().longType()
      is Schema.Int32 -> SchemaBuilder.builder().intType()
      is Schema.Float64 -> SchemaBuilder.builder().doubleType()
      is Schema.Float32 -> SchemaBuilder.builder().floatType()
      is Schema.Decimal -> LogicalTypes.decimal(schema.precision.value, schema.scale.value)
        .addToSchema(SchemaBuilder.builder().bytesType())
      is Schema.Array -> SchemaBuilder.builder().array().items(toAvro(schema.elements))
      is Schema.Enum -> SchemaBuilder.enumeration("enum").symbols(*schema.symbols.toTypedArray())
      is Schema.TimestampMillis -> LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType())
      is Schema.Struct -> {
        val builder = SchemaBuilder.record(schema.name).fields()
        schema.fields.fold(builder) { acc, op -> acc.name(op.name).type(toAvro(op.schema)).noDefault() }.endRecord()
      }
      else -> error("Unsupported schema $schema")
    }
  }

  fun fromAvro(schema: AvroSchema): Schema {
    return when (schema.type) {
      org.apache.avro.Schema.Type.RECORD -> {
        val fields = schema.fields.map { Schema.Field(it.name(), fromAvro(it.schema())) }
        Schema.Struct(schema.name, fields)
      }
      org.apache.avro.Schema.Type.ENUM -> Schema.Enum(schema.enumSymbols)
      org.apache.avro.Schema.Type.ARRAY -> Schema.Array(fromAvro(schema.elementType))
      org.apache.avro.Schema.Type.MAP -> TODO()
      org.apache.avro.Schema.Type.UNION -> TODO()
      org.apache.avro.Schema.Type.FIXED -> TODO()
      org.apache.avro.Schema.Type.STRING -> Schema.Strings
      org.apache.avro.Schema.Type.BYTES -> when (val lt = schema.logicalType) {
        is LogicalTypes.Decimal -> Schema.Decimal(Schema.Precision(lt.precision), Schema.Scale(lt.scale))
        else -> Schema.Bytes
      }
      org.apache.avro.Schema.Type.INT -> Schema.Int32
      org.apache.avro.Schema.Type.LONG -> when (val lt = schema.logicalType) {
        is LogicalTypes.TimestampMillis -> Schema.TimestampMillis
        else -> Schema.Int64
      }
      org.apache.avro.Schema.Type.FLOAT -> Schema.Float32
      org.apache.avro.Schema.Type.DOUBLE -> Schema.Float64
      org.apache.avro.Schema.Type.BOOLEAN -> Schema.Booleans
      org.apache.avro.Schema.Type.NULL -> TODO()
    }
  }
}
