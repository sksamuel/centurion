dependencies {
   api(project(Projects.schemas))
   implementation(Libs.Orc.core)
}

apply("../publish.gradle.kts")
