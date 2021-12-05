package com.sksamuel.centurion

import com.sksamuel.centurion.HiveTestConfig.fs
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.fs.Path

class StagingFileManagerTest : FunSpec() {

  init {
    test("complete should set visible") {
      StagingFileManager().complete(Path("wibble/.foo"), fs) shouldBe Path("wibble/foo")
    }
  }

}
