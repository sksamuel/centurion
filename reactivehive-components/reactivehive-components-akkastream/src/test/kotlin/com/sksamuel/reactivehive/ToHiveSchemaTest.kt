package com.sksamuel.reactivehive

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
      ToHiveSchema.toHiveType(DecimalType(Precision(12), Scale(4))) shouldBe "decimal(12,4)"
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
  }

}