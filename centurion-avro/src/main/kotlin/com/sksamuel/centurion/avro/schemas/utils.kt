package com.sksamuel.centurion.avro.schemas

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData

val STRING_SCHEMA: Schema = SchemaBuilder.builder().stringType()
val STRING_SCHEMA_JAVA: Schema = SchemaBuilder.builder().stringType().also {
   GenericData.setStringType(it, GenericData.StringType.String)
}

fun Schema.asNullUnion(): Schema = SchemaBuilder.unionOf().nullType().and().type(this).endUnion()
