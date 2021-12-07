@file:Suppress("RECEIVER_NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")

package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.nullable
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.File
import java.sql.Timestamp
import java.time.Instant

class ParquetReaderTest : FunSpec() {

  private val conf: Configuration = Configuration()
  private val fs: FileSystem = FileSystem.getLocal(conf)

  init {

    test("reading an existing parquet file") {

      val path = Path("file://" + this.javaClass.getResource("/userdata.parquet").file)
      val reader = Parquet.reader(path, conf)
      val struct1 = reader.read()

      struct1.schema shouldBe Schema.Struct(
        "hive_schema",
        Schema.Field("registration_dttm", Schema.TimestampMillis.nullable()),
        Schema.Field("id", Schema.Int32.nullable()),
        Schema.Field("first_name", Schema.Strings.nullable()),
        Schema.Field("last_name", Schema.Strings.nullable()),
        Schema.Field("email", Schema.Strings.nullable()),
        Schema.Field("gender", Schema.Strings.nullable()),
        Schema.Field("ip_address", Schema.Strings.nullable()),
        Schema.Field("cc", Schema.Strings.nullable()),
        Schema.Field("country", Schema.Strings.nullable()),
        Schema.Field("birthdate", Schema.Strings.nullable()),
        Schema.Field("salary", Schema.Float64.nullable()),
        Schema.Field("title", Schema.Strings.nullable()),
        Schema.Field("comments", Schema.Strings.nullable()),
      )

      struct1.values shouldBe listOf(
        Timestamp.from(Instant.ofEpochMilli(1454529329000)),
        1,
        "Amanda",
        "Jordan",
        "ajordan0@com.com",
        "Female",
        "1.197.201.2",
        "6759521864920116",
        "Indonesia",
        "3/8/1971",
        49756.53,
        "Internal Auditor",
        "1E+02"
      )

      val struct2 = reader.read()

      struct2.values shouldBe
        listOf(
          Timestamp.from(Instant.ofEpochMilli(1454562243000)),
          2,
          "Albert",
          "Freeman",
          "afreeman1@is.gd",
          "Male",
          "218.111.175.34",
          "",
          "Canada",
          "1/16/1968",
          150280.17,
          "Accountant IV",
          ""
        )
    }

    test("reading arrays and maps") {
      val reader = Parquet.reader(File(this.javaClass.getResource("/map_array.parquet").file), conf)
      val struct = reader.read()
      struct.schema shouldBe Schema.Struct(
        name = "spark_schema",
        fields = listOf(
          Schema.Field(
            name = "map_op_op",
            schema = Schema.Map(Schema.Strings, Schema.Strings.nullable()).nullable(),
          ),
          Schema.Field(
            name = "map_op_req",
            schema = Schema.Map(Schema.Strings, Schema.Strings).nullable(),
          ),
          Schema.Field(
            name = "map_req_op",
            schema = Schema.Map(Schema.Strings, Schema.Strings.nullable()),
          ),
          Schema.Field(
            name = "map_req_req",
            schema = Schema.Map(Schema.Strings, Schema.Strings),
          ),
          Schema.Field(
            name = "arr_op_op",
            schema = Schema.Array(Schema.Strings.nullable()).nullable(),
          ),
          Schema.Field(
            name = "arr_op_req",
            schema = Schema.Array(Schema.Strings).nullable(),
          ),
          Schema.Field(
            name = "arr_req_op",
            schema = Schema.Array(Schema.Strings.nullable()),
          ),
          Schema.Field(
            name = "arr_req_req",
            schema = Schema.Array(Schema.Strings),
          ),
        )
      )

      val expected = listOf(
        mapOf("wilma" to "benji", "fred" to "benji"),
        mapOf("wilma" to "mighty", "fred" to "mighty"),
        emptyMap<String, Any?>(),
        mapOf("wilma" to "mighty", "barney" to "benji"),
        listOf("mighty"),
        listOf("benji", "franky"),
        emptyList<Any?>(),
        listOf("mighty"),
      )

      struct.values shouldBe expected
    }
  }
}
