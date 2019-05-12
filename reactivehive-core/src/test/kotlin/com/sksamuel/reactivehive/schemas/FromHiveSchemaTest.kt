package com.sksamuel.reactivehive.schemas

import com.sksamuel.reactivehive.ArrayType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.CharType
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.DecimalType
import com.sksamuel.reactivehive.Float32Type
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.Int16Type
import com.sksamuel.reactivehive.Int64Type
import com.sksamuel.reactivehive.Int8Type
import com.sksamuel.reactivehive.Precision
import com.sksamuel.reactivehive.Scale
import com.sksamuel.reactivehive.StringType
import com.sksamuel.reactivehive.StructField
import com.sksamuel.reactivehive.StructType
import com.sksamuel.reactivehive.VarcharType
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec

class FromHiveSchemaTest : FunSpec() {
  init {
    test("support string") {
      FromHiveSchema.fromHiveType("string") shouldBe StringType
    }

    test("support tinyint") {
      FromHiveSchema.fromHiveType("tinyint") shouldBe Int8Type
    }

    test("support smallint") {
      FromHiveSchema.fromHiveType("smallint") shouldBe Int16Type
    }

    test("support bigint") {
      FromHiveSchema.fromHiveType("bigint") shouldBe Int64Type
    }

    test("support double") {
      FromHiveSchema.fromHiveType("double") shouldBe Float64Type
    }

    test("support float") {
      FromHiveSchema.fromHiveType("float") shouldBe Float32Type
    }

    test("support boolean") {
      FromHiveSchema.fromHiveType("boolean") shouldBe BooleanType
    }

    test("support date") {
      FromHiveSchema.fromHiveType("date") shouldBe DateType
    }

    test("support varchar") {
      FromHiveSchema.fromHiveType("varchar(3)") shouldBe VarcharType(3)
      FromHiveSchema.fromHiveType("varchar(  3  )") shouldBe VarcharType(3)
    }

    test("support char") {
      FromHiveSchema.fromHiveType("char(3)") shouldBe CharType(3)
      FromHiveSchema.fromHiveType("char(  3  )") shouldBe CharType(3)
    }

    test("support decimal") {
      FromHiveSchema.fromHiveType("decimal(3,4)") shouldBe DecimalType(Precision(
          3), Scale(4))
      FromHiveSchema.fromHiveType("decimal(  3,   4)") shouldBe DecimalType(Precision(
          3), Scale(4))
      FromHiveSchema.fromHiveType("decimal(  3  ,   4  )") shouldBe DecimalType(Precision(
          3), Scale(4))
      FromHiveSchema.fromHiveType("decimal(  3  ,4  )") shouldBe DecimalType(Precision(
          3), Scale(4))
    }

    test("support arrays") {
      FromHiveSchema.fromHiveType("array<string>") shouldBe ArrayType(StringType)
      FromHiveSchema.fromHiveType("array<    boolean    >") shouldBe ArrayType(BooleanType)
      FromHiveSchema.fromHiveType("array<bigint    >") shouldBe ArrayType(Int64Type)
      FromHiveSchema.fromHiveType("array<   float>") shouldBe ArrayType(Float32Type)
    }

    test("support structs") {
      FromHiveSchema.fromHiveType("struct<a:boolean, b:float>") shouldBe
          StructType(StructField("a",
              BooleanType),
              StructField("b", Float32Type))
      FromHiveSchema.fromHiveType("struct<  a   : boolean  , b   : float  >") shouldBe
          StructType(StructField("a",
              BooleanType),
              StructField("b", Float32Type))
      FromHiveSchema.fromHiveType("struct< a:string, b:array<boolean>>") shouldBe
          StructType(StructField("a",
              StringType),
              StructField("b",
                  ArrayType(BooleanType)))
    }

    test("!nested structs") {
      FromHiveSchema.fromHiveType("struct<a:boolean, b:float, c:struct<d:string, e:tinyint>>") shouldBe
          StructType(
              StructField("a", BooleanType),
              StructField("b", Float32Type),
              StructField("c",
                  StructType(
                      StructField("d", StringType),
                      StructField("e", Int8Type)
                  )
              )
          )
    }
  }
}