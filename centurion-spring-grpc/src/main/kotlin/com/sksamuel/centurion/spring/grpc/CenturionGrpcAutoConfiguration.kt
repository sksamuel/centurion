package com.sksamuel.centurion.spring.grpc

import io.grpc.ServerServiceDefinition
import io.grpc.netty.NettyServerBuilder
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.context.annotation.Bean
import org.springframework.grpc.server.ServerBuilderCustomizer

/**
 * Spring Boot auto-configuration that exposes an [AvroServerBuilderCustomizer] bean collecting
 * every [ServerServiceDefinition] in the application context.
 *
 * Users register their Avro-marshalled services as `ServerServiceDefinition` beans (built via
 * [AvroMethodDescriptors] or by hand) and this auto-configuration wires them into the gRPC
 * server. If no [ServerServiceDefinition] beans are defined, the customizer is a no-op.
 *
 * The bean is typed for [NettyServerBuilder], which Spring gRPC's server starter brings in
 * transitively. Users running on a different server impl can define their own customizer for
 * that builder type.
 */
@AutoConfiguration
@ConditionalOnClass(ServerBuilderCustomizer::class, NettyServerBuilder::class)
class CenturionGrpcAutoConfiguration {

   @Bean
   fun avroServerBuilderCustomizer(
      definitions: List<ServerServiceDefinition>,
   ): AvroServerBuilderCustomizer<NettyServerBuilder> = AvroServerBuilderCustomizer(definitions)
}
