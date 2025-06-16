dependencies {
   api(project(":centurion-avro"))
   api(libs.lettuce.core)
   api(libs.avro)
   implementation(libs.kotlinx.coroutines.core)
   testApi(libs.testcontainers.redis)
   testApi(libs.kotest.testcontainers)
}

apply("../publish.gradle.kts")
