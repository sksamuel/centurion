package com.sksamuel.reactivehive

import com.sksamuel.reactivehive.HiveTestConfig.client
import io.kotlintest.matchers.collections.shouldContain
import io.kotlintest.specs.FunSpec

class HiveUtilsTest : FunSpec() {

  private val utils = HiveUtils(client)

  init {
    test("list databases") {
      utils.listDatabases().shouldContain(DatabaseName("default"))
    }
  }
}