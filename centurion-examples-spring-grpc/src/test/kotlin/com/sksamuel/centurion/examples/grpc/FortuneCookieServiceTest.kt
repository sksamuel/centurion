package com.sksamuel.centurion.examples.grpc

import com.sksamuel.centurion.spring.grpc.AvroMethodDescriptors
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.stub.ClientCalls
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldNotBeEmpty
import java.util.UUID

class FortuneCookieServiceTest : FunSpec({

   lateinit var server: Server
   lateinit var channel: ManagedChannel

   beforeSpec {
      val serverName = "fortune-test-${UUID.randomUUID()}"
      server = InProcessServerBuilder.forName(serverName)
         .directExecutor()
         .addService(FortuneCookieService().bindService())
         .build()
         .start()
      channel = InProcessChannelBuilder.forName(serverName).directExecutor().build()
   }

   afterSpec {
      channel.shutdownNow()
      server.shutdownNow()
   }

   val pick = AvroMethodDescriptors.unary<FortuneRequest, FortuneResponse>(
      "${FortuneCookieService.SERVICE_NAME}/${FortuneCookieService.PICK_METHOD}",
   )

   test("Pick returns a non-empty fortune") {
      val response = ClientCalls.blockingUnaryCall(channel, pick, io.grpc.CallOptions.DEFAULT, FortuneRequest())
      response.fortune.shouldNotBeEmpty()
   }

   test("Pick returns a fortune from the configured list") {
      val response = ClientCalls.blockingUnaryCall(channel, pick, io.grpc.CallOptions.DEFAULT, FortuneRequest())
      FortuneCookieService.DEFAULT_FORTUNES shouldContain response.fortune
   }

   test("Pick is deterministic for a fixed seed") {
      val first = ClientCalls.blockingUnaryCall(channel, pick, io.grpc.CallOptions.DEFAULT, FortuneRequest(seed = 42L))
      val second = ClientCalls.blockingUnaryCall(channel, pick, io.grpc.CallOptions.DEFAULT, FortuneRequest(seed = 42L))
      first.fortune shouldBe second.fortune
   }
})
