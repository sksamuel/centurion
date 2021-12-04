package com.sksamuel.centurion.resolver

import com.sksamuel.centurion.BooleanType
import com.sksamuel.centurion.Float64Type
import com.sksamuel.centurion.StringType
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructField
import com.sksamuel.centurion.StructType
import io.kotlintest.shouldThrowAny
import io.kotlintest.specs.FunSpec

class StrictStructResolverTest : FunSpec() {

  val schema = StructType(
      StructField("name", StringType),
      StructField("title", StringType),
      StructField("salary", Float64Type),
      StructField("employed", BooleanType)
  )

  val user = Struct(schema, "sam", "mr", 100.43, false)

  init {

    test("fail if the metastore schema has an extra field than the struct schema") {

      val metastoreSchema = StructType(
          StructField("name", StringType),
          StructField("city", StringType),
          StructField("title", StringType),
          StructField("salary", Float64Type),
          StructField("employed", BooleanType)
      )

      shouldThrowAny {
        StrictStructResolver.resolve(user, metastoreSchema)
      }
    }


    test("fail if the struct schema has an extra field than the metastore schema") {

      val metastoreSchema = StructType(
          StructField("title", StringType),
          StructField("salary", Float64Type),
          StructField("employed", BooleanType)
      )

      shouldThrowAny {
        StrictStructResolver.resolve(user, metastoreSchema)
      }
    }
  }
}
