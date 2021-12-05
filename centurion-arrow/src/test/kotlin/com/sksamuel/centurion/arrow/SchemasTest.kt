package com.sksamuel.centurion.arrow

import com.sksamuel.centurion.Schema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType

class SchemasTest : FunSpec({

  test("centurion <--> arrow strings") {
    Schemas.fromArrow(ArrowType.Utf8()) shouldBe Schema.Strings
    Schemas.toArrow(Schema.Strings) shouldBe ArrowType.Utf8()
  }

  test("centurion <--> arrow integers") {
    Schemas.fromArrow(ArrowType.Int(64, true)) shouldBe Schema.Int64
    Schemas.fromArrow(ArrowType.Int(32, true)) shouldBe Schema.Int32
    Schemas.fromArrow(ArrowType.Int(16, true)) shouldBe Schema.Int16
    Schemas.fromArrow(ArrowType.Int(8, true)) shouldBe Schema.Int8

    Schemas.toArrow(Schema.Int64) shouldBe ArrowType.Int(64, true)
    Schemas.toArrow(Schema.Int32) shouldBe ArrowType.Int(32, true)
    Schemas.toArrow(Schema.Int16) shouldBe ArrowType.Int(16, true)
    Schemas.toArrow(Schema.Int8) shouldBe ArrowType.Int(8, true)
  }

  test("centurion <--> arrow Booleans") {
    Schemas.fromArrow(ArrowType.Bool()) shouldBe Schema.Booleans
    Schemas.toArrow(Schema.Booleans) shouldBe ArrowType.Bool()
  }

  test("centurion <--> arrow Binary") {
    Schemas.fromArrow(ArrowType.Binary()) shouldBe Schema.Bytes
    Schemas.toArrow(Schema.Bytes) shouldBe ArrowType.Binary()
  }

  test("centurion <--> arrow struct") {

    val strField = Field("col1", FieldType.nullable(ArrowType.Utf8()), null)
    val intField = Field("col2", FieldType.nullable(ArrowType.Int(32, true)), null);
    val schema = org.apache.arrow.vector.types.pojo.Schema(listOf(strField, intField))

    val struct = Schema.Struct("struct", Schema.Field("col1", Schema.Strings), Schema.Field("col2", Schema.Int32))

    Schemas.fromArrow(schema) shouldBe struct
    Schemas.toArrowSchema(struct) shouldBe schema
  }
})
