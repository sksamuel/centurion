package com.sksamuel.centurion.avro.io.serde

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import com.sksamuel.centurion.avro.encoders.Encoder
import com.sksamuel.centurion.avro.encoders.SpecificReflectionRecordEncoder
import com.sksamuel.centurion.avro.io.BinaryReader
import com.sksamuel.centurion.avro.io.BinaryWriter
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.reflect.KClass

/**
 * A [BinarySerde] reads and writes in the avro "binary" format which does not include the schema
 * in the written bytes.
 *
 * This results in a smaller payload compared to [DataSerde], similar to protobuf,
 * but requires that the schema is provided at deserialization type. This format is especially
 * effective when the consumers and producers can agree on the schema used, for instance in RPC style services.
 */
class BinarySerde<T : Any>(
  private val schema: Schema,
  private val encoder: Encoder<T>,
  private val decoder: Decoder<T>,
  private val encoderFactory: EncoderFactory,
  private val decoderFactory: DecoderFactory,
) : Serde<T> {

  companion object {
    inline operator fun <reified T : Any> invoke(
      encoderFactory: EncoderFactory,
      decoderFactory: DecoderFactory,
    ): BinarySerde<T> {
      return invoke(T::class, encoderFactory, decoderFactory)
    }

    operator fun <T : Any> invoke(
      kclass: KClass<T>,
      encoderFactory: EncoderFactory,
      decoderFactory: DecoderFactory,
    ): BinarySerde<T> {
      val schema = ReflectionSchemaBuilder(true).schema(kclass)
      val encoder = SpecificReflectionRecordEncoder<T>()
      val decoder = ReflectionRecordDecoder<T>(kclass)
      return BinarySerde(schema, encoder, decoder, encoderFactory, decoderFactory)
    }
  }

  override fun serialize(obj: T): ByteArray {
    val baos = ByteArrayOutputStream()
    val writer = BinaryWriter(schema, baos, encoder, encoderFactory, null)
    writer.use { writer.write(obj) }
    return baos.toByteArray()
  }

  override fun deserialize(bytes: ByteArray): T {
    return BinaryReader(schema, ByteArrayInputStream(bytes), decoderFactory, decoder, null).use { it.read() }
  }
}
