package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.decoders.Decoder
import com.sksamuel.centurion.avro.decoders.SpecificRecordDecoder
import com.sksamuel.centurion.avro.encoders.Encoder
import com.sksamuel.centurion.avro.encoders.SpecificRecordEncoder
import com.sksamuel.centurion.avro.generation.ReflectionSchemaBuilder
import org.apache.avro.Schema
import org.apache.avro.file.Codec
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import kotlin.reflect.KClass

/**
 * A [Serde] provides an easy way to convert between data classes and avro encoded bytes.
 *
 * This class is thread safe.
 */
class Serde<T : Any>(
   private val schema: Schema,
   encoder: Encoder<T>,
   private val decoder: Decoder<T>,
   private val options: SerdeOptions,
) {

   init {
      if (options.fastReader)
         GenericData.get().setFastReaderEnabled(true)
   }

   companion object {

      /**
       * Creates a [Schema] reflectively from the given [kclass] using a [ReflectionSchemaBuilder].
       */
      operator fun <T : Any> invoke(
         kclass: KClass<T>,
         options: SerdeOptions = SerdeOptions()
      ): Serde<T> {
         val schema = ReflectionSchemaBuilder(true).schema(kclass)
         val encoder = SpecificRecordEncoder(kclass, schema)
         val decoder: SpecificRecordDecoder<T> = SpecificRecordDecoder(kclass, schema)
         return Serde(schema, encoder, decoder, options)
      }

      /**
       * Creates a [Schema] reflectively from the given type parameter [T] using a [ReflectionSchemaBuilder].
       */
      inline operator fun <reified T : Any> invoke(options: SerdeOptions = SerdeOptions()): Serde<T> {
         return Serde(T::class, options)
      }
   }

   private val encoderFactory = EncoderFactory()
      .configureBufferSize(options.encoderBufferSize)
      .configureBlockSize(options.blockBufferSize)

   private val decoderFactory = DecoderFactory()
      .configureDecoderBufferSize(options.decoderBufferSize)

   private val writerFactory = BinaryWriterFactory(schema, encoderFactory)
   private val readerFactory = BinaryReaderFactory(schema, decoderFactory)

   private val encodeFn = encoder.encode(schema)

   fun serialize(obj: T): ByteArray = writerFactory.write(encodeFn.invoke(obj) as GenericRecord, options.codec)
   fun deserialize(bytes: ByteArray): T = decoder.decode(schema, readerFactory.read(bytes, options.codec))
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
