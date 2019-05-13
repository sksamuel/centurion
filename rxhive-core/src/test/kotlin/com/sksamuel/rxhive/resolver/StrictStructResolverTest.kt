package com.sksamuel.rxhive.resolver

import com.sksamuel.rxhive.BooleanType
import com.sksamuel.rxhive.Float64Type
import com.sksamuel.rxhive.StringType
import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructField
import com.sksamuel.rxhive.StructType
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