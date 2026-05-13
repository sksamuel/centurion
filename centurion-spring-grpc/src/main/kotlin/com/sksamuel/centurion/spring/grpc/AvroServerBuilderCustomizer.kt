package com.sksamuel.centurion.spring.grpc

import io.grpc.ServerBuilder
import io.grpc.ServerServiceDefinition
import org.springframework.grpc.server.ServerBuilderCustomizer

/**
 * A [ServerBuilderCustomizer] that adds a fixed list of [ServerServiceDefinition]s to the
 * gRPC server. Typical use: build service definitions whose method descriptors use
 * Avro-backed [AvroMarshaller]s (e.g. via [AvroMethodDescriptors]) and register them as Spring
 * beans; this customizer picks them up and attaches them to the configured gRPC server.
 *
 * Generic over the concrete builder type to satisfy [ServerBuilderCustomizer]'s recursive bound;
 * the [customize] body only calls [ServerBuilder.addService], which is defined on the base
 * class, so any concrete builder works.
 *
 * If [definitions] is empty, this customizer is a no-op.
 */
class AvroServerBuilderCustomizer<T : ServerBuilder<T>>(
   private val definitions: List<ServerServiceDefinition>,
) : ServerBuilderCustomizer<T> {

   override fun customize(serverBuilder: T) {
      for (definition in definitions) {
         serverBuilder.addService(definition)
      }
   }
}
