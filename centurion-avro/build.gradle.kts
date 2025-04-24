dependencies {
   api(project(Projects.schemas))
   implementation(libs.avro)
   testImplementation(libs.jackson.module.kotlin)
}

apply("../publish.gradle.kts")
