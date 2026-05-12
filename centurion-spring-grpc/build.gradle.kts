plugins {
   id("kotlin-conventions")
   id("publishing-conventions")
}

dependencies {
   api(projects.centurionAvro)
   api(libs.avro)
   api(libs.spring.grpc.server.starter)
}
