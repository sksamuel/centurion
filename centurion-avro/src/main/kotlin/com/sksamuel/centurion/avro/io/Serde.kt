package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.encoders.Encoder
import org.apache.avro.Schema
import org.apache.avro.file.Codec
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

/**
 * A [Serde] provides an easy way to convert between a specific data class [T] and avro encoded bytes
 * by delegating to an [Encoder] and [Decoder] that handles that type.
 *
 * If you wish to create a [Serde] reflectively, see [ReflectionSerdeFactory].
 *
 * This class is thread safe once constructed.
 */
class Serde<T : Any>(
   schema: Schema,
   encoder: Encoder<T>,
   decoder: Decoder<T>,
   private val options: SerdeOptions,
) {

   init {
      if (options.fastReader)
         GenericData.get().setFastReaderEnabled(true)
   }

   private val encoderFactory = EncoderFactory()
      .configureBufferSize(options.encoderBufferSize)
      .configureBlockSize(options.blockBufferSize)

   private val decoderFactory = DecoderFactory()
      .configureDecoderBufferSize(options.decoderBufferSize)

   private val writerFactory = BinaryWriterFactory(schema, encoderFactory)
   private val readerFactory = BinaryReaderFactory(schema, decoderFactory)

   private val encodeFn = encoder.encode(schema)
   private val decodeFn = decoder.decode(schema)

   fun serialize(obj: T): ByteArray = writerFactory.write(encodeFn.invoke(obj) as GenericRecord, options.codec)
   fun deserialize(bytes: ByteArray): T = decodeFn(readerFactory.read(bytes, options.codec))
}

private const val DEFAULT_ENCODER_BUFFER_SIZE = 2048
private const val DEFAULT_DECODER_BUFFER_SIZE = 8192
private const val DEFAULT_BLOCK_BUFFER_SIZE = 64 * 1024

data class SerdeOptions(
   val fastReader: Boolean = false,
   val encoderBufferSize: Int = DEFAULT_ENCODER_BUFFER_SIZE,
   val decoderBufferSize: Int = DEFAULT_DECODER_BUFFER_SIZE,
   val blockBufferSize: Int = DEFAULT_BLOCK_BUFFER_SIZE,
   val codec: Codec? = null,
)
