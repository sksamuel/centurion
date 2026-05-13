package com.sksamuel.centurion.examples.grpc

/**
 * Domain types for the example `example.FortuneCookie` gRPC service.
 *
 * These are plain Kotlin data classes. [com.sksamuel.centurion.spring.grpc.AvroMarshaller]
 * builds reflection-based encoders/decoders for them at runtime; the same wire format
 * is used on the client and server because they share these classes.
 */
data class FortuneRequest(
   val seed: Long = 0L,
)

data class FortuneResponse(
   val fortune: String,
)
