package com.sksamuel.rxhive.parquet

import com.sksamuel.rxhive.BooleanType
import com.sksamuel.rxhive.Int32Type
import com.sksamuel.rxhive.StringType
import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.StructField
import com.sksamuel.rxhive.StructType
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types

class ParquetWriterTest : FunSpec() {

  init {

    val conf = Configuration()
    val fs = FileSystem.getLocal(conf)

    test("StructParquetWriter should write a single struct") {

      val path = Path("test.pq")
      if (fs.exists(path))
        fs.delete(path, false)

      val schema = StructType(StructField("a", StringType), StructField("b", Int32Type), StructField("c", BooleanType))
      val struct = Struct(schema, "a", 1, true)
      val messageType = ToParquetSchema.toMessageType(schema, "element")

      val writer = parquetWriter(path, conf, messageType)
      writer.write(struct)
      writer.close()

      val input = HadoopInputFile.fromPath(path, conf)
      ParquetFileReader.open(input).fileMetaData.schema shouldBe
          Types.buildMessage()
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).`as`(OriginalType.UTF8).named("a"))
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL).named("b"))
              .addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL).named("c"))
              .named("element")
    }

    test("should support overwrite") {

      val schema = StructType(StructField("a", StringType), StructField("b", Int32Type), StructField("c", BooleanType))
      val struct = Struct(schema, "a", 1, true)
      val messageType = ToParquetSchema.toMessageType(schema, "element")

      val path = Path("test.pq")
      fs.exists(path) shouldBe true

      val writer = parquetWriter(path, conf, messageType, true)
      writer.write(struct)
      writer.close()
    }
  }
}