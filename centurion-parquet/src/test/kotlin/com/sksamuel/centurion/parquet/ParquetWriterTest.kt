package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.nullable
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.sql.Timestamp
import java.time.Instant

class ParquetWriterTest : FunSpec() {

  init {

    val conf = Configuration()
    val fs = FileSystem.getLocal(conf)

    test("Parquet writer should write a single struct") {

      val path = Path("test.pq")
      if (fs.exists(path))
        fs.delete(path, false)
      fs.deleteOnExit(path)

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Int32),
        Schema.Field("c", Schema.Booleans)
      )

      val writer = Parquet.writer(path, conf, schema)
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

    test("Parquet writer should write TimestampMillis") {

      val path = Path("test.pq")
      if (fs.exists(path))
        fs.delete(path, false)
      fs.deleteOnExit(path)

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.TimestampMillis),
        Schema.Field("b", Schema.TimestampMillis),
      )

      // we write out a timestamp, but it will be read back in as an instant
      val struct = Struct(schema, Timestamp.from(Instant.ofEpochSecond(123)), Instant.ofEpochSecond(456))

      val writer = Parquet.writer(path, conf, schema)
      writer.write(struct)
      writer.close()

      Parquet.reader(path, conf).read() shouldBe Struct(
        schema,
        Instant.ofEpochSecond(123),
        Instant.ofEpochSecond(456)
      )
    }

    test("should support overwrite") {

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Int32),
        Schema.Field("c", Schema.Booleans)
      )
      val path = Path("test.pq")
      fs.exists(path) shouldBe true
      fs.deleteOnExit(path)

      val writer = Parquet.writer(path, conf, schema, ParquetFileWriter.Mode.OVERWRITE)
      writer.write(Struct(schema, "a", 1, true))
      writer.close()
    }

    test("writer should support arrays of primitives") {

      val schema = Schema.Struct(
        "myrecord",
        Schema.Field("a", Schema.Strings),
        Schema.Field("b", Schema.Array(Schema.Int32)),
        Schema.Field("c", Schema.Booleans)
      )

      val path = Path("test_array.pq")
      fs.deleteOnExit(path)

      val writer = Parquet.writer(path, conf, schema, ParquetFileWriter.Mode.OVERWRITE)
      writer.write(Struct(schema, "a", listOf(1, 2, 3), true))
      writer.close()

      Parquet.reader(path, conf).read() shouldBe Struct(schema, "a", listOf(1, 2, 3), true)
    }

    test("writer should support arrays of structs") {

      val nested = Schema.Struct(
        "x",
        Schema.Field("c", Schema.Int32.nullable()),
        Schema.Field("d", Schema.Float32),
      )

      val schema = Schema.Struct("y", Schema.Field("b", Schema.Array(nested)))

      val path = Path("test_array.pq")
      fs.deleteOnExit(path)

      val writer = Parquet.writer(path, conf, schema, ParquetFileWriter.Mode.OVERWRITE)
      writer.write(
        Struct(
          schema,
          listOf(
            listOf(
              Struct(nested, listOf(123, 1.2)),
              Struct(nested, listOf(345, 1.3))
            )
          )
        )
      )
      writer.close()

      val struct = Parquet.reader(path, conf).read()

      struct.schema shouldBe Schema.Struct(
        "y",
        Schema.Field(
          "b",
          Schema.Array(
            Schema.Struct(
              "element",
              Schema.Field("c", Schema.Int32.nullable()),
              Schema.Field("d", Schema.Float32),
            )
          )
        ),
      )

      val items = struct.values[0] as List<Struct>
      items[0].values shouldBe listOf(123, 1.2F)
      items[1].values shouldBe listOf(345, 1.3F)
    }

    test("writer should support nested structs") {

      val inner = Schema.Struct(
        "inner",
        Schema.Field("b", Schema.Int32.nullable()),
        Schema.Field("c", Schema.Booleans),
      )

      val outer = Schema.Struct("outer", Schema.Field("a", inner))

      val path = Path("test_nested.pq")
      fs.deleteOnExit(path)

      val writer = Parquet.writer(path, conf, outer, ParquetFileWriter.Mode.OVERWRITE)
      writer.write(
        Struct(
          outer,
          listOf(
            Struct(inner, listOf(345, true))
          )
        )
      )
      writer.close()

      val struct = Parquet.reader(path, conf).read()

      struct.schema shouldBe Schema.Struct(
        "outer",
        Schema.Field(
          "a",
          Schema.Struct(
            "a",
            Schema.Field("b", Schema.Int32.nullable()),
            Schema.Field("c", Schema.Booleans),
          )
        ),
      )

      val items = struct.values[0] as Struct
      items.values shouldBe listOf(345, true)
    }
  }
}
