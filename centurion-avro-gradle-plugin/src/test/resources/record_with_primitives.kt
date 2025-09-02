package com.sksamuel.centurion

import kotlin.Int
import kotlin.String
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8

public data class Foo(
  public val a: Int,
  public val b: String,
)
