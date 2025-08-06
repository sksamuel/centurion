plugins {
   id("kotlin-conventions")
   id("publishing-conventions")
}

dependencies {
   api(projects.centurionAvro)
   api(libs.lettuce.core)
   api(libs.avro)
   implementation(libs.kotlinx.coroutines.core)
   testApi(libs.testcontainers.redis)
}
