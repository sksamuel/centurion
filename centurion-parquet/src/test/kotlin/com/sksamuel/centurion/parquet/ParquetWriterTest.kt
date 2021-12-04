package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.parquet.schemas.ToParquetSchema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

class ParquetWriterTest : FunSpec() {

  init {

    val conf = Configuration()
    val fs = FileSystem.getLocal(conf)

    test("Parquet writer should write a single struct") {

      val path = Path("test.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Int32),
        Schema.Field("c", Schema.Booleans)
      )
      val messageType = ToParquetSchema.toMessageType(schema)

      val writer = Parquet.writer(path, conf, messageType)
      writer.write(Struct(schema, "a", 1, true))
      writer.close()

      val input = HadoopInputFile.fromPath(path, conf)
      ParquetFileReader.open(input).fileMetaData.schema shouldBe
        Types.buildMessage()
          .addField(
            Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
              .`as`(LogicalTypeAnnotation.stringType())
              .named("a")
          )
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).named("b"))
          .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED).named("c"))
          .named("myrecord")
    }

    test("should support overwrite") {

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Int32),
        Schema.Field("c", Schema.Booleans)
      )
      val messageType = ToParquetSchema.toMessageType(schema)

      val path = Path("test.pq")
      fs.exists(path) shouldBe true

      val writer = Parquet.writer(path, conf, messageType, true)
      writer.write(Struct(schema, "a", 1, true))
      writer.close()
    }

    test("!writer should support arrays of primitives") {

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Array(Schema.Int32)),
        Schema.Field("c", Schema.Booleans)
      )

      val messageType = ToParquetSchema.toMessageType(schema)

      val path = Path("test_array.pq")
      fs.deleteOnExit(path)

      val writer = Parquet.writer(path, conf, messageType, true)
      writer.write(Struct(schema, "a", listOf(1, 2, 3), true))
      writer.close()

      Parquet.reader(path, conf).read() shouldBe Struct(schema, "a", listOf(1, 2, 3), true)
    }

    test("writer should support arrays of objects") {

    }
  }
}
