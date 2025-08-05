plugins {
   id("me.champeau.jmh") version "0.7.3"
   id("kotlin-conventions")
   id("publishing-conventions")
}

dependencies {
   implementation(libs.avro)
   implementation(libs.kotlinx.coroutines.core)
   implementation(libs.commons.pool)
   testImplementation(libs.jackson.module.kotlin)
   testImplementation("org.xerial.snappy:snappy-java:1.1.10.7")
   testImplementation("com.ning:compress-lzf:1.1.2")
   testImplementation("com.github.luben:zstd-jni:1.5.7-3:linux_amd64")
}

jmh {
   jmhVersion = "1.37"
   duplicateClassesStrategy = DuplicatesStrategy.WARN
   fork = 2
   warmupIterations = 3
   iterations = 5
   threads = 1
}
