dependencies {
   api(project(Projects.schemas))
   implementation("org.apache.avro:avro:1.10.2")
}

apply("../publish.gradle.kts")
