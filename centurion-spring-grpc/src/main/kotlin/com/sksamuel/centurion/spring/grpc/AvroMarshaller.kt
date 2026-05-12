package com.sksamuel.centurion.spring.grpc

import com.sksamuel.centurion.avro.io.serde.BinarySerde
import com.sksamuel.centurion.avro.io.serde.Serde
import io.grpc.MethodDescriptor
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayInputStream
import java.io.InputStream
import kotlin.reflect.KClass

/**
 * A gRPC [MethodDescriptor.Marshaller] that serializes / deserializes values of type [T] using
 * a centurion-avro [Serde].
 *
 * Use as the request or response marshaller of a [MethodDescriptor]. By default this wraps a
 * [BinarySerde], which produces the same payload shape as protobuf in terms of "schema is implicit,
 * payload is just bytes". See [forBinary] and [forSerde] for factory methods.
 *
 * The underlying [Serde] is expected to be thread-safe; the marshaller itself adds no extra state.
 */
class AvroMarshaller<T : Any>(private val serde: Serde<T>) : MethodDescriptor.Marshaller<T> {

   override fun stream(value: T): InputStream = ByteArrayInputStream(serde.serialize(value))

   override fun parse(stream: InputStream): T = serde.deserialize(stream.readAllBytes())

   companion object {

      /**
       * Builds an [AvroMarshaller] backed by a [BinarySerde] for [T] using reflection
       * to generate the schema, encoder, and decoder.
       */
      inline fun <reified T : Any> forBinary(
         encoderFactory: EncoderFactory = EncoderFactory.get(),
         decoderFactory: DecoderFactory = DecoderFactory.get(),
      ): AvroMarshaller<T> = forBinary(T::class, encoderFactory, decoderFactory)

      /**
       * Builds an [AvroMarshaller] backed by a [BinarySerde] for [kclass] using reflection
       * to generate the schema, encoder, and decoder.
       */
      fun <T : Any> forBinary(
         kclass: KClass<T>,
         encoderFactory: EncoderFactory = EncoderFactory.get(),
         decoderFactory: DecoderFactory = DecoderFactory.get(),
      ): AvroMarshaller<T> = AvroMarshaller(BinarySerde(kclass, encoderFactory, decoderFactory))

      /**
       * Builds an [AvroMarshaller] backed by a caller-provided [Serde].
       */
      fun <T : Any> forSerde(serde: Serde<T>): AvroMarshaller<T> = AvroMarshaller(serde)
   }
}
