package com.sksamuel.reactivehive.schemas

import com.sksamuel.reactivehive.ArrayType
import com.sksamuel.reactivehive.BinaryType
import com.sksamuel.reactivehive.BooleanType
import com.sksamuel.reactivehive.CharType
import com.sksamuel.reactivehive.DateType
import com.sksamuel.reactivehive.DecimalType
import com.sksamuel.reactivehive.Float32Type
import com.sksamuel.reactivehive.Float64Type
import com.sksamuel.reactivehive.Int16Type
import com.sksamuel.reactivehive.Int32Type
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