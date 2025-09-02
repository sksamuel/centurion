import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
   `java-library`
   kotlin("jvm")
}

group = Libs.org
version = Ci.version

java {
   toolchain {
      languageVersion.set(JavaLanguageVersion.of(17))
   }
   sourceCompatibility = JavaVersion.VERSION_17
   targetCompatibility = JavaVersion.VERSION_17
   withSourcesJar()
}

tasks.test {
   useJUnitPlatform()
   testLogging {
      showExceptions = true
      showStandardStreams = true
      exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
   }
}

kotlin {
   compilerOptions {
      jvmTarget.set(JvmTarget.JVM_17)
      apiVersion.set(KotlinVersion.KOTLIN_2_2)
      languageVersion.set(KotlinVersion.KOTLIN_2_2)
      freeCompilerArgs.set(listOf("-Xwhen-guards"))
   }
}

dependencies {
   testImplementation("org.jetbrains.kotlin:kotlin-stdlib:2.2.10")

   val kotestVersion = "6.0.2"
   testImplementation("io.kotest:kotest-framework-engine:$kotestVersion")
   testImplementation("io.kotest:kotest-runner-junit5:$kotestVersion")
   testImplementation("io.kotest:kotest-assertions-core:$kotestVersion")
   testImplementation("io.kotest:kotest-assertions-json:$kotestVersion")
   testImplementation("io.kotest:kotest-extensions-testcontainers:$kotestVersion")
   testImplementation("io.kotest:kotest-property:$kotestVersion")
}
