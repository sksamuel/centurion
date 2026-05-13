package com.sksamuel.centurion.ktor

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation as ServerContentNegotiation
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication

data class Greeting(val from: String, val to: String, val seq: Int)

class AvroContentConverterTest : FunSpec({

   test("server-side serialize: a GET endpoint produces application/avro bytes that the client decodes") {
      testApplication {
         application {
            install(ServerContentNegotiation) { avro() }
            routing {
               get("/greeting") {
                  call.respond(Greeting("Ada", "Grace", 42))
               }
            }
         }
         val client = createClient {
            install(ClientContentNegotiation) { avro() }
         }
         val response = client.get("/greeting")
         response.status shouldBe HttpStatusCode.OK
         response.contentType()?.contentType shouldBe "application"
         response.contentType()?.contentSubtype shouldBe "avro"
         response.body<Greeting>() shouldBe Greeting("Ada", "Grace", 42)
      }
   }

   test("server-side deserialize: a POST endpoint receives an Avro body sent by the client") {
      testApplication {
         application {
            install(ServerContentNegotiation) { avro() }
            routing {
               post("/echo") {
                  val incoming = call.receive<Greeting>()
                  call.respond(incoming.copy(seq = incoming.seq + 1))
               }
            }
         }
         val client = createClient {
            install(ClientContentNegotiation) { avro() }
         }
         val response = client.post("/echo") {
            contentType(AvroContentType)
            setBody(Greeting("Linus", "Ken", 1))
         }
         response.status shouldBe HttpStatusCode.OK
         response.body<Greeting>() shouldBe Greeting("Linus", "Ken", 2)
      }
   }

   test("the serde cache reuses the underlying serde across requests for the same type") {
      testApplication {
         application {
            install(ServerContentNegotiation) { avro() }
            routing {
               get("/ping/{seq}") {
                  val seq = call.parameters["seq"]!!.toInt()
                  call.respond(Greeting("server", "client", seq))
               }
            }
         }
         val client = createClient {
            install(ClientContentNegotiation) { avro() }
         }
         for (seq in 1..5) {
            client.get("/ping/$seq").body<Greeting>() shouldBe Greeting("server", "client", seq)
         }
      }
   }
})
