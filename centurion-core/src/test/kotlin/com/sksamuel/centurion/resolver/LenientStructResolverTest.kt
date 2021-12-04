package com.sksamuel.centurion.resolver

import com.sksamuel.centurion.BooleanType
import com.sksamuel.centurion.Float64Type
import com.sksamuel.centurion.StringType
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.StructField
import com.sksamuel.centurion.StructType
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec

class LenientStructResolverTest : FunSpec() {

  val schema = StructType(
      StructField("name", StringType),
      StructField("title", StringType),
      StructField("salary", Float64Type),
      StructField("employed", BooleanType)
  )

  val user = Struct(schema, "sam", "mr", 100.43, false)

  init {
    test("LenientStructResolver should pad missing fields") {

      val metastoreSchema = StructType(
          StructField("name", StringType),
          StructField("title", StringType),
          StructField("salary", Float64Type),
          StructField("employed", BooleanType),
          StructField("city", StringType)
      )

      LenientStructResolver.resolve(user, metastoreSchema) shouldBe Struct(
          StructType(
              StructField("name", StringType, true),
              StructField("title", StringType, true),
              StructField("salary", Float64Type, true),
              StructField("employed", BooleanType, true),
              StructField("city", StringType, true)
          ),
          listOf("sam", "mr", 100.43, false, null)
      )
    }

    test("LenientStructResolver should remove extraneous fields") {

      val metastoreSchema = StructType(
          StructField("name", StringType),
          StructField("title", StringType),
          StructField("salary", Float64Type)
      )

      LenientStructResolver.resolve(user, metastoreSchema) shouldBe Struct(
          StructType(
              StructField("name", StringType, true),
              StructField("title", StringType, true),
              StructField("salary", Float64Type, true)
          ),
          listOf("sam", "mr", 100.43)
      )
    }
  }
}
