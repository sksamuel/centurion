dependencies {
   api(project(Projects.schemas))
   implementation("org.apache.avro:avro:1.11.3")
}

apply("../publish.gradle.kts")
