package com.sksamuel.centurion.orc

import com.sksamuel.centurion.Schema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.orc.TypeDescription

class SchemasTest : FunSpec({

  test("orc varchars") {
    val type = TypeDescription.createVarchar().withMaxLength(33)
    val schema = Schema.Varchar(33)
    Schemas.fromOrc(type) shouldBe schema
    Schemas.toOrc(schema) shouldBe type
  }

  test("orc numbers") {
    val type = TypeDescription.createStruct()
      .addField("byte", TypeDescription.createByte())
      .addField("short", TypeDescription.createShort())
      .addField("int", TypeDescription.createInt())
      .addField("long", TypeDescription.createLong())
      .addField("float", TypeDescription.createFloat())
      .addField("double", TypeDescription.createDouble())

    val schema = Schema.Struct(
      "struct",
      Schema.Field("byte", Schema.Int8),
      Schema.Field("short", Schema.Int16),
      Schema.Field("int", Schema.Int32),
      Schema.Field("long", Schema.Int64),
      Schema.Field("float", Schema.Float32),
      Schema.Field("double", Schema.Float64),
    )

    Schemas.fromOrc(type) shouldBe schema
    Schemas.toOrc(schema) shouldBe type
  }

  test("orc TypeDescriptions should map to Centurion schemas") {

    val type = TypeDescription.createStruct()
      .addField("array", TypeDescription.createList(TypeDescription.createBoolean()))
      .addField("map", TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createInt()))
      .addField("binary", TypeDescription.createBinary())
      .addField("boolean", TypeDescription.createBoolean())
      .addField("string", TypeDescription.createString())
      .addField("timestamp", TypeDescription.createTimestamp())
      .addField(
        "struct",
        TypeDescription.createStruct()
          .addField("a", TypeDescription.createLong())
          .addField("b", TypeDescription.createString())
      )

    val schema = Schema.Struct(
      "struct",
      Schema.Field("array", Schema.Array(Schema.Booleans)),
      Schema.Field("map", Schema.Map(Schema.Int32)),
      Schema.Field("binary", Schema.Bytes),
      Schema.Field("boolean", Schema.Booleans),
      Schema.Field("string", Schema.Strings),
      Schema.Field("timestamp", Schema.TimestampMillis),
      Schema.Field(
        "struct",
        Schema.Struct(
          "struct",
          Schema.Field("a", Schema.Int64),
          Schema.Field("b", Schema.Strings)
        )
      )
    )

    Schemas.fromOrc(type) shouldBe schema
    Schemas.toOrc(schema) shouldBe type
  }

  test("enums") {
    Schemas.toOrc(Schema.Enum("enum", "malbec", "shiraz")) shouldBe TypeDescription.createString()
  }

  test("decimals") {
    Schemas.toOrc(Schema.Decimal(Schema.Precision(5), Schema.Scale(2))) shouldBe
      TypeDescription.createDecimal().withScale(2).withPrecision(5)

    Schemas.fromOrc(TypeDescription.createDecimal().withScale(2).withPrecision(5)) shouldBe
      Schema.Decimal(Schema.Precision(5), Schema.Scale(2))
  }
})
