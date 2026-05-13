package com.sksamuel.centurion.examples.grpc

import com.sksamuel.centurion.spring.grpc.AvroMethodDescriptors
import io.grpc.ServerCallHandler
import io.grpc.ServerServiceDefinition
import io.grpc.stub.ServerCalls
import kotlin.random.Random

/**
 * Server-side implementation of the `example.FortuneCookie` service.
 *
 * Exposes a single unary RPC, `Pick`, that returns a randomly chosen fortune. If the
 * request carries a non-zero `seed`, the random source is seeded with it so callers
 * can get deterministic responses (useful for tests).
 */
class FortuneCookieService(
   private val fortunes: List<String> = DEFAULT_FORTUNES,
) {

   /**
    * Build a [ServerServiceDefinition] for `example.FortuneCookie` with Avro marshallers.
    *
    * The descriptor's full method name is `example.FortuneCookie/Pick`. Both the request
    * and response wire types are Avro binary, schemaless — see [AvroMethodDescriptors].
    */
   fun bindService(): ServerServiceDefinition {
      val pick = AvroMethodDescriptors.unary<FortuneRequest, FortuneResponse>(
         fullMethodName = "$SERVICE_NAME/$PICK_METHOD",
      )

      val pickHandler: ServerCallHandler<FortuneRequest, FortuneResponse> =
         ServerCalls.asyncUnaryCall { request, responseObserver ->
            val random = if (request.seed == 0L) Random.Default else Random(request.seed)
            val fortune = fortunes[random.nextInt(fortunes.size)]
            responseObserver.onNext(FortuneResponse(fortune))
            responseObserver.onCompleted()
         }

      return ServerServiceDefinition.builder(SERVICE_NAME)
         .addMethod(pick, pickHandler)
         .build()
   }

   companion object {
      const val SERVICE_NAME = "example.FortuneCookie"
      const val PICK_METHOD = "Pick"

      val DEFAULT_FORTUNES: List<String> = listOf(
         "A bold venture begins today.",
         "Good things come to those who automate.",
         "Reuse a serde — your future self will thank you.",
         "Beware of the off-by-one error.",
         "Read the schema. Trust the schema.",
         "Today is a good day to ship a small PR.",
         "The cache that is never invalidated is no cache at all.",
         "Patience is a virtue. Idempotence is a feature.",
         "A green build is worth a thousand hopes.",
         "Comments lie. Tests don't.",
      )
   }
}
