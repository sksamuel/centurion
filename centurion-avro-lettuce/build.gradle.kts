dependencies {
   api(project(":centurion-avro"))
   api(libs.lettuce.core)
   api(libs.avro)
   implementation(libs.kotlinx.coroutines.core)
}

apply("../publish.gradle.kts")
