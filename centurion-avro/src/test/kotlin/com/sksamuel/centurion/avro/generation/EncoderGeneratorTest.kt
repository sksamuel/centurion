package com.sksamuel.centurion.avro.generation

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.avro.SchemaBuilder

class EncoderGeneratorTest : FunSpec() {
   init {
      test("generate encoder for primitive types") {

         val schema = SchemaBuilder.record("Foo")
            .namespace("com.example")
            .fields()
            .requiredString("a")
            .requiredBoolean("b")
            .optionalLong("c")
            .optionalInt("d")
            .requiredDouble("e")
            .requiredFloat("f")
            .endRecord()

         val generator = EncoderGenerator()
         val generatedCode = generator.generate(schema)

         val expectedCode = """
package com.example

import com.sksamuel.centurion.avro.encoders.*
import org.apache.avro.generic.GenericData

val fooEncoder: Encoder<Foo> = Encoder<Foo> { schema ->
  {
    val record = GenericData.Record(schema)
    record.put("a", StringEncoder.encode(it.a))
    record.put("b", it.b)
    record.put("c", it.c)
    record.put("d", it.d)
    record.put("e", it.e)
    record.put("f", it.f)
    record
  }
}
""".trim()

         generatedCode.trim() shouldBe expectedCode
      }

      test("generate encoder for long arrays") {

         val schema = SchemaBuilder.record("Bar")
            .namespace("com.example")
            .fields()
            .name("longArray")
            .type()
            .array()
            .items(SchemaBuilder.builder().longType())
            .noDefault()
            .endRecord()

         val generator = EncoderGenerator()
         val generatedCode = generator.generate(schema)

         val expectedCode = """
package com.example

import com.sksamuel.centurion.avro.encoders.*
import org.apache.avro.generic.GenericData

val barEncoder: Encoder<Bar> = Encoder<Bar> { schema ->
  {
    val record = GenericData.Record(schema)
    record.put("longArray", LongArrayEncoder.encode(it.longArray))
    record
  }
}
""".trim()

         generatedCode.trim() shouldBe expectedCode
      }

      test("generate encoder for enums") {

         val schema = SchemaBuilder.record("Baz")
            .namespace("com.example")
            .fields()
            .name("enumField")
            .type()
            .enumeration("SampleEnum")
            .symbols("VALUE1", "VALUE2", "VALUE3")
            .noDefault()
            .endRecord()

         val generator = EncoderGenerator()
         val generatedCode = generator.generate(schema)

         val expectedCode = """
package com.example

import com.sksamuel.centurion.avro.encoders.*
import org.apache.avro.generic.GenericData

val bazEncoder: Encoder<Baz> = Encoder<Baz> { schema ->
  {
    val record = GenericData.Record(schema)
    record.put("enumField", EnumEncoder.encode(it.enumField))
    record
  }
}
   """.trim()

         generatedCode.trim() shouldBe expectedCode
      }
   }
}
