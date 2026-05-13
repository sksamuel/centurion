package com.sksamuel.centurion.examples.grpc

import io.grpc.ServerServiceDefinition
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean

/**
 * Sample Spring Boot application that exposes the `example.FortuneCookie` gRPC service
 * with Avro-encoded request/response payloads via centurion-spring-grpc.
 *
 * On startup, Spring Boot picks up the `CenturionGrpcAutoConfiguration` from
 * `centurion-spring-grpc`, which collects every `ServerServiceDefinition` bean — including
 * the one defined below — and attaches them to the embedded gRPC server.
 *
 * Run with `./gradlew :centurion-examples-spring-grpc:bootRun` (or the equivalent
 * `main` entry point in your IDE); the service listens on whatever port Spring gRPC's
 * starter is configured for (default 9090 — see `application.yml`).
 */
@SpringBootApplication
class FortuneCookieApplication {

   @Bean
   fun fortuneCookieService(): FortuneCookieService = FortuneCookieService()

   /**
    * Expose the service binding as a [ServerServiceDefinition] bean. The auto-configuration
    * picks every such bean up and registers it on the gRPC server builder.
    */
   @Bean
   fun fortuneCookieServiceDefinition(service: FortuneCookieService): ServerServiceDefinition =
      service.bindService()
}

fun main(args: Array<String>) {
   runApplication<FortuneCookieApplication>(*args)
}
