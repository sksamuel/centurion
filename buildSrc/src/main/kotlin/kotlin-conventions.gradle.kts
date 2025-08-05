import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
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

kotlin {
   compilerOptions {
      jvmTarget.set(JvmTarget.JVM_17)
      apiVersion.set(KotlinVersion.KOTLIN_2_2)
      languageVersion.set(KotlinVersion.KOTLIN_2_2)
   }
}

dependencies {
   testImplementation("org.jetbrains.kotlin:kotlin-stdlib:2.1.21")
   testImplementation("io.kotest:kotest-runner-junit5:5.9.1")
   testImplementation("io.kotest:kotest-assertions-core:5.9.1")
   testImplementation("io.kotest:kotest-assertions-json:5.9.1")
   testImplementation("io.kotest:kotest-framework-datatest:5.9.1")
   testImplementation("io.kotest:kotest-property:5.9.1")
}

tasks.named<Test>("test") {
   useJUnitPlatform()
   filter {
      isFailOnNoMatchingTests = false
   }
   testLogging {
      showExceptions = true
      showStandardStreams = true
      events = setOf(
         TestLogEvent.FAILED,
         TestLogEvent.PASSED
      )
      exceptionFormat = TestExceptionFormat.FULL
   }
}
