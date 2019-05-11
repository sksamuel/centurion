package com.sksamuel.reactivehive

import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import org.apache.hadoop.fs.Path

class DefaultPartitionLocatorTest : FunSpec() {

  init {
    test("should use hive style paths") {
      DefaultPartitionLocator.path(
          Path("foo/bar"),
          Partition(PartitionPart(PartitionKey("state"), "il"), PartitionPart(PartitionKey("country"), "us"))
      ) shouldBe Path("foo/bar/state=il/country=us")
    }
  }

}