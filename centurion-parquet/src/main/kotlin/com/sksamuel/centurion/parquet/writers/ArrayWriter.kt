package com.sksamuel.centurion.parquet.writers

import com.sksamuel.centurion.Schema
import org.apache.parquet.io.api.RecordConsumer

class ArrayWriter(schema: Schema.Array) : Writer {
  private val writer = Writer.writerFor(schema.elements)
  override fun write(consumer: RecordConsumer, value: Any) {

    val elements = when (value) {
      is List<*> -> value
      is Array<*> -> value.asList()
      else -> error("Unsupported array type ${value::class}")
    }

    // this follows the spark convention: an array is a group that contains a field 'list' that itself
    // contains repeated groups, one per array item, which contains a field 'element' only if the array item is not null
    // https://github.com/apache/spark/blob/ef5278f7a1637950b9eee06a4c82325d6ef607c1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetWriteSupport.scala#L329
    consumer.startGroup()
    // Only creates the repeated field if the array is non-empty.
    if (elements.isNotEmpty()) {
      consumer.startField("list", 0)
      elements.forEach {
        consumer.startGroup()
        // Only creates the element field if the current array element is not null.
        if (it != null) {
          consumer.startField("element", 0)
          writer.write(consumer, it)
          consumer.endField("element", 0)
        }
        consumer.endGroup()
      }
      consumer.endField("list", 0)
    }
    consumer.endGroup()
  }
}
