import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
   kotlin("jvm")
}

allprojects {
   apply(plugin = "org.jetbrains.kotlin.jvm")

   group = Libs.org
   version = Ci.version

   dependencies {
      testImplementation(rootProject.libs.bundles.testing)
   }

   tasks.named<Test>("test") {
      useJUnitPlatform()
      testLogging {
         showExceptions = true
         showStandardStreams = true
         exceptionFormat = TestExceptionFormat.FULL
      }
   }

   kotlin {
      compilerOptions {
         jvmTarget = JvmTarget.JVM_17
         apiVersion = KotlinVersion.KOTLIN_2_1
         languageVersion = KotlinVersion.KOTLIN_2_1
         freeCompilerArgs.set(listOf("-Xwhen-guards"))
      }
   }
}
