package com.sksamuel.centurion.parquet.writers

import org.apache.parquet.io.api.RecordConsumer

fun RecordConsumer.writeGroup(fn: () -> Unit) {
  startGroup()
  fn()
  endGroup()
}

// top level types must be wrapped in messages
fun RecordConsumer.writeMessage(fn: () -> Unit) {
  startMessage()
  fn()
  endMessage()
}

fun RecordConsumer.writeField(name: String, index: Int, fn: () -> Unit) {
  startField(name, index)
  fn()
  endField(name, index)
}
