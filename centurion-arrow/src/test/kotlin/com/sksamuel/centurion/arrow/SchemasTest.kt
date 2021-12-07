package com.sksamuel.centurion.arrow

import com.sksamuel.centurion.Schema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit
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

  test("floats") {
    Schemas.fromArrow(ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)) shouldBe Schema.Float64
    Schemas.fromArrow(ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)) shouldBe Schema.Float32

    Schemas.toArrow(Schema.Float64) shouldBe ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    Schemas.toArrow(Schema.Float32) shouldBe ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
  }

  test("centurion <--> arrow Booleans") {
    Schemas.fromArrow(ArrowType.Bool()) shouldBe Schema.Booleans
    Schemas.toArrow(Schema.Booleans) shouldBe ArrowType.Bool()
  }

  test("centurion <--> arrow Binary") {
    Schemas.fromArrow(ArrowType.Binary()) shouldBe Schema.Bytes
    Schemas.toArrow(Schema.Bytes) shouldBe ArrowType.Binary()
  }

  test("centurion <--> arrow timestamp millis") {
    Schemas.fromArrow(ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")) shouldBe Schema.TimestampMillis
    Schemas.toArrow(Schema.TimestampMillis) shouldBe ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
  }

  test("centurion <--> arrow decimal") {
    Schemas.fromArrow(ArrowType.Decimal(5, 2, 128)) shouldBe Schema.Decimal(Schema.Precision(5), Schema.Scale(2))
    Schemas.toArrow(Schema.Decimal(Schema.Precision(5), Schema.Scale(2))) shouldBe ArrowType.Decimal(5, 2, 128)
  }

  test("enums") {
    Schemas.toArrow(Schema.Enum("enum", "malbec", "shiraz")) shouldBe ArrowType.Utf8()
  }

  test("centurion <--> arrow struct") {

    val strField = Field("col1", FieldType.nullable(ArrowType.Utf8()), null)
    val intField = Field("col2", FieldType.nullable(ArrowType.Int(32, true)), null);
    val schema = org.apache.arrow.vector.types.pojo.Schema(listOf(strField, intField))

    val struct = Schema.Struct("struct", Schema.Field("col1", Schema.Strings), Schema.Field("col2", Schema.Int32))

    Schemas.fromArrow(schema) shouldBe struct
    Schemas.toArrowSchema(struct) shouldBe schema
  }

  test("timestamp millis") {
    Schemas.toArrow(Schema.TimestampMillis) shouldBe ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
    Schemas.fromArrow(ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")) shouldBe Schema.TimestampMillis
  }

  test("timestamp micros") {
    Schemas.toArrow(Schema.TimestampMicros) shouldBe ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")
    Schemas.fromArrow(ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")) shouldBe Schema.TimestampMicros
  }

  test("maps") {
    val type = ArrowType.Map(false)
    val schema = Schema.Map(Schema.Strings, Schema.Strings)
    Schemas.toArrow(schema) shouldBe type
    Schemas.fromArrow(type) shouldBe schema
  }
})
