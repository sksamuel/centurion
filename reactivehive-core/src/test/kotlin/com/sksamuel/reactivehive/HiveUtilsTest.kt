package com.sksamuel.reactivehive

import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.specs.FunSpec

class HiveUtilsTest : FunSpec(), HiveTestConfig {

  private val utils = HiveUtils(client)

  init {
    test("list databases") {
      utils.listDatabases().shouldContain(DatabaseName("default"))
    }
  }
}