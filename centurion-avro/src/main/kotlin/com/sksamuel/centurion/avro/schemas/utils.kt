package com.sksamuel.centurion.avro.schemas

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

fun Schema.asNullUnion(): Schema = SchemaBuilder.unionOf().nullType().and().type(this).endUnion()
