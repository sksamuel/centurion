plugins {
   id("kotlin-conventions")
   id("publishing-conventions")
}

dependencies {
   api(projects.centurionAvro)
   api(libs.avro)
   api(libs.ktor.serialization)

   testImplementation(libs.ktor.server.test.host)
   testImplementation(libs.ktor.server.content.negotiation)
   testImplementation(libs.ktor.client.content.negotiation)
   testImplementation(libs.ktor.client.core)
}
