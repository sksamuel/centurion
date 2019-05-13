package com.sksamuel.reactivehive.resolver

import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.Struct
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
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