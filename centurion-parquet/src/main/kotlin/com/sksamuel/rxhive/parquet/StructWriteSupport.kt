package com.sksamuel.rxhive.parquet

import com.sksamuel.rxhive.Struct
import com.sksamuel.rxhive.parquet.setters.StructSetter
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import java.math.RoundingMode

class StructWriteSupport(private val schema: MessageType,
                         private val roundingMode: RoundingMode,
                         private val metadata: Map<String, String>) : WriteSupport<Struct>() {

  private var consumer: RecordConsumer? = null

  override fun init(configuration: Configuration?) = WriteContext(schema, metadata)
  override fun finalizeWrite(): FinalizedWriteContext = FinalizedWriteContext(metadata)

  override fun prepareForWrite(consumer: RecordConsumer) {
    this.consumer = consumer
  }

  override fun write(struct: Struct) {
    val setter = StructSetter(struct.schema, RoundingMode.UNNECESSARY, true)
    // prepare must have been called by the contract of the parquet library
    setter.set(consumer!!, struct)
  }
}