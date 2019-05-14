package com.sksamuel.rxhive

import com.sksamuel.rxhive.HiveTestConfig.fs
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