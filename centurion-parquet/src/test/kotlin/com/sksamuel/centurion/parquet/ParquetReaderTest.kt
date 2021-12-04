package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Schema
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
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

      struct1.schema shouldBe Schema.Record(
        "hive_schema",
        Schema.Field("registration_dttm", Schema.TimestampMillis, true),
        Schema.Field("id", Schema.Int32, true),
        Schema.Field("first_name", Schema.Strings, true),
        Schema.Field("last_name", Schema.Strings, true),
        Schema.Field("email", Schema.Strings, true),
        Schema.Field("gender", Schema.Strings, true),
        Schema.Field("ip_address", Schema.Strings, true),
        Schema.Field("cc", Schema.Strings, true),
        Schema.Field("country", Schema.Strings, true),
        Schema.Field("birthdate", Schema.Strings, true),
        Schema.Field("salary", Schema.Float64, true),
        Schema.Field("title", Schema.Strings, true),
        Schema.Field("comments", Schema.Strings, true)
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
  }
}
