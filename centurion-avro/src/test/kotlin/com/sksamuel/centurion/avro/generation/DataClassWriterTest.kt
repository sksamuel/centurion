package com.sksamuel.centurion.avro.generation

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

class DataClassWriterTest : FunSpec({

   test("should write all types") {
      DataClassWriter.write(
         DataClass(
            packageName = "com.sksamuel",
            className = "Foo",
            members = listOf(
               Member("a", Type.StringType),
               Member("b", Type.BooleanType),
               Member("c", Type.LongType),
               Member("d", Type.IntType),
               Member("e", Type.FloatType),
               Member("f", Type.DoubleType),
               Member("g", Type.RecordType("x", "y")),
            )
         )
      ).trim() shouldBe
         """
package com.sksamuel

data class Foo(
  val a: String,
  val b: Boolean,
  val c: Long,
  val d: Int,
  val e: Float,
  val f: Double,
  val g: x.y,
)
""".trim()
   }

})
