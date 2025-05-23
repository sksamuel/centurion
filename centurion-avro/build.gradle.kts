dependencies {
   api(project(Projects.schemas))
   implementation(libs.avro)
   implementation(libs.kotlinx.coroutines.core)
   testImplementation(libs.jackson.module.kotlin)
   testImplementation("org.xerial.snappy:snappy-java:1.1.10.7")
   testImplementation("com.ning:compress-lzf:1.1.2")
   testImplementation("com.github.luben:zstd-jni:1.5.7-3:linux_amd64")
}

apply("../publish.gradle.kts")
