package com.sksamuel.rxhive.schemas

import com.sksamuel.rxhive.ArrayType
import com.sksamuel.rxhive.BinaryType
import com.sksamuel.rxhive.BooleanType
import com.sksamuel.rxhive.CharType
import com.sksamuel.rxhive.DateType
import com.sksamuel.rxhive.DecimalType
import com.sksamuel.rxhive.Float32Type
import com.sksamuel.rxhive.Float64Type
import com.sksamuel.rxhive.Int16Type
import com.sksamuel.rxhive.Int32Type
import com.sksamuel.rxhive.Int64Type
import com.sksamuel.rxhive.Int8Type
import com.sksamuel.rxhive.Precision
import com.sksamuel.rxhive.Scale
import com.sksamuel.rxhive.StringType
import com.sksamuel.rxhive.StructField
import com.sksamuel.rxhive.StructType
import com.sksamuel.rxhive.VarcharType
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec

class ToHiveSchemaTest : FunSpec() {

  init {

    test("should support strings") {
      ToHiveSchema.toHiveType(StringType) shouldBe "string"
    }

    test("should support date") {
      ToHiveSchema.toHiveType(DateType) shouldBe "date"
    }

    test("should support blobs") {
      ToHiveSchema.toHiveType(BinaryType) shouldBe "binary"
    }

    test("should support decimal") {
      ToHiveSchema.toHiveType(DecimalType(Precision(
          12), Scale(4))) shouldBe "decimal(12,4)"
    }

    test("should support booleans") {
      ToHiveSchema.toHiveType(BooleanType) shouldBe "boolean"
    }

    test("should support ints") {
      ToHiveSchema.toHiveType(Int64Type) shouldBe "bigint"
      ToHiveSchema.toHiveType(Int32Type) shouldBe "int"
      ToHiveSchema.toHiveType(Int16Type) shouldBe "smallint"
      ToHiveSchema.toHiveType(Int8Type) shouldBe "tinyint"
    }

    test("should support floats") {
      ToHiveSchema.toHiveType(Float64Type) shouldBe "double"
      ToHiveSchema.toHiveType(Float32Type) shouldBe "float"
    }

    test("should support chars") {
      ToHiveSchema.toHiveType(VarcharType(123)) shouldBe "varchar(123)"
      ToHiveSchema.toHiveType(CharType(53)) shouldBe "char(53)"
    }

    test("should support arrays") {
      ToHiveSchema.toHiveType(
          StructType(
              StructField("a", StringType),
              StructField("b",
                  ArrayType(BooleanType))
          )
      ) shouldBe "struct<a:string,b:array<boolean>>"
    }

    test("should support structs") {
      ToHiveSchema.toHiveType(StructType(StructField("a",
          StringType))) shouldBe "struct<a:string>"
    }

    test("nested structs") {
      ToHiveSchema.toHiveType(
          StructType(
              StructField("a", StringType),
              StructField("b",
                  StructType(StructField("c",
                      BooleanType)))
          )
      ) shouldBe "struct<a:string,b:struct<c:boolean>>"

      ToHiveSchema.toHiveType(
          StructType(
              StructField("a", StringType),
              StructField("b",
                  ArrayType(BooleanType))
          )
      ) shouldBe "struct<a:string,b:array<boolean>>"
    }
  }

}