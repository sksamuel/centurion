plugins {
   id("kotlin-conventions")
}

dependencies {
   implementation(projects.centurionSpringGrpc)
   implementation(libs.spring.grpc.server.starter)
   implementation("org.springframework.boot:spring-boot-starter:3.4.5")
   implementation("io.grpc:grpc-stub:1.72.0")

   testImplementation("org.springframework.boot:spring-boot-starter-test:3.4.5")
   testImplementation("org.springframework.grpc:spring-grpc-test:0.8.0")
   testImplementation("io.grpc:grpc-inprocess:1.72.0")
}
