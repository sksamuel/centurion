package com.sksamuel.centurion.schemas

import com.sksamuel.centurion.ArrayType
import com.sksamuel.centurion.BinaryType
import com.sksamuel.centurion.BooleanType
import com.sksamuel.centurion.CharType
import com.sksamuel.centurion.DateType
import com.sksamuel.centurion.DecimalType
import com.sksamuel.centurion.Float32Type
import com.sksamuel.centurion.Float64Type
import com.sksamuel.centurion.Int16Type
import com.sksamuel.centurion.Int32Type
import com.sksamuel.centurion.Int64Type
import com.sksamuel.centurion.Int8Type
import com.sksamuel.centurion.Precision
import com.sksamuel.centurion.Scale
import com.sksamuel.centurion.StringType
import com.sksamuel.centurion.StructField
import com.sksamuel.centurion.StructType
import com.sksamuel.centurion.VarcharType
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
