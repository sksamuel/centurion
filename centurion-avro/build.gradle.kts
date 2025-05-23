dependencies {
   api(project(Projects.schemas))
   implementation(libs.avro)
   implementation(libs.kotlinx.coroutines.core)
   testImplementation(libs.jackson.module.kotlin)
}

apply("../publish.gradle.kts")
