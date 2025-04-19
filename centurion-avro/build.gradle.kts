dependencies {
   api(project(Projects.schemas))
   implementation(libs.avro)
   api("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
}

apply("../publish.gradle.kts")
