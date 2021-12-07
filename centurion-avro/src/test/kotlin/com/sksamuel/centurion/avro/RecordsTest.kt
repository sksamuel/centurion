package com.sksamuel.centurion.avro

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.util.Utf8

class RecordsTest : FunSpec() {
  init {
    test("converting struct to generic record") {
      val record = Records.toGenericRecord(
        Struct(
          Schema.Struct(
            "mystruct",
            Schema.Field("a", Schema.Strings),
            Schema.Field("b", Schema.Booleans),
            Schema.Field("c", Schema.Int64),
            Schema.Field("d", Schema.Int32),
          ),
          listOf("foo", true, 435L, 123)
        ),
      )
      record.get("a") shouldBe Utf8("foo")
      record.get("b") shouldBe true
      record.get("c") shouldBe 435L
      record.get("d") shouldBe 123
    }
  }
}
