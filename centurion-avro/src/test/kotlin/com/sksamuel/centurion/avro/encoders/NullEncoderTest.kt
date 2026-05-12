package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.schemas.asNullUnion
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.Schema
import org.apache.avro.util.Utf8

class NullEncoderTest : FunSpec({

   val nullableString = Schema.create(Schema.Type.STRING).asNullUnion()

   test("encodes null as null") {
      NullEncoder(StringEncoder).encode(nullableString, null) shouldBe null
   }

   test("delegates non-null values to the wrapped encoder") {
      NullEncoder(StringEncoder).encode(nullableString, "hello") shouldBe Utf8("hello")
   }

   test("requires a UNION schema") {
      val schema = Schema.create(Schema.Type.STRING)
      shouldThrow<IllegalArgumentException> {
         NullEncoder(StringEncoder).encode(schema, "x")
      }
   }

   test("requires a 2-element union") {
      val schema = Schema.createUnion(
         Schema.create(Schema.Type.NULL),
         Schema.create(Schema.Type.STRING),
         Schema.create(Schema.Type.INT),
      )
      shouldThrow<IllegalArgumentException> {
         NullEncoder(StringEncoder).encode(schema, "x")
      }
   }
})
