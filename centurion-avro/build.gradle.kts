dependencies {
   api(project(Projects.schemas))
   testImplementation(libs.bundles.testing)
   implementation(libs.avro)
}

apply("../publish.gradle.kts")
