dependencies {
   api(project(Projects.schemas))
   implementation("org.apache.arrow:arrow-vector:6.0.1")
}

apply("../publish.gradle.kts")
