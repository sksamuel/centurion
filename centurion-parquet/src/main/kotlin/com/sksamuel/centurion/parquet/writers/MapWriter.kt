package com.sksamuel.centurion.parquet.writers

import com.sksamuel.centurion.Schema
import org.apache.parquet.io.api.RecordConsumer

class MapWriter(schema: Schema.Map) : Writer {

  private val keyWriter = Writer.writerFor(Schema.Strings)
  private val valueWriter = Writer.writerFor(schema.values)

  /**
   * This writer follows the spark convention:
   * https://github.com/apache/spark/blob/ef5278f7a1637950b9eee06a4c82325d6ef607c1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetWriteSupport.scala#L441
   */
  override fun write(consumer: RecordConsumer, value: Any) {

    val map = value as Map<String, *>

    //   <map-repetition> group <map-name> (MAP) {
    //     repeated group key_value {
    //       required <key-type> key;
    //       <value-repetition> <value-type> value;
    //     }
    //   }
    consumer.writeGroup {
      if (map.isNotEmpty()) {
        consumer.writeField("key_value", 0) {
          map.forEach { (key, value) ->
            consumer.writeGroup {
              consumer.writeField("key", 0) {
                keyWriter.write(consumer, key)
              }
              if (value != null) {
                consumer.writeField("value", 1) {
                  valueWriter.write(consumer, value)
                }
              }
            }
          }
        }
      }
    }
  }
}
