dependencies {
   api(project(Projects.schemas))
   implementation(libs.avro)
}

apply("../publish.gradle.kts")
