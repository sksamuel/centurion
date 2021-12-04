package com.sksamuel.centurion.parquet

import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.parquet.writers.StructWriter
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import java.math.RoundingMode

/**
 * Converts incoming records from centurion structs to parquet.
 *
 * @param metadata arbitrary key-value pairs included in the footer of the file.
 */
internal class StructWriteSupport(
  private val schema: MessageType,
  private val roundingMode: RoundingMode,
  private val metadata: Map<String, String>
) : WriteSupport<Struct>() {

  private var consumer: RecordConsumer? = null

  override fun init(configuration: Configuration) = WriteContext(schema, metadata)
  override fun finalizeWrite(): FinalizedWriteContext = FinalizedWriteContext(metadata)

  /**
   * This will be called once per row group.
   * The [consumer] abstracts writing columns from the domain model.
   */
  override fun prepareForWrite(consumer: RecordConsumer) {
    this.consumer = consumer
  }

  override fun write(struct: Struct) {
    val writer = StructWriter(struct.schema, RoundingMode.UNNECESSARY, true)
    writer.write(consumer ?: error("prepareForWrite must have been called by parquet"), struct)
  }
}
