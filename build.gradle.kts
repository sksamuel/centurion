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

   java {
      targetCompatibility = JavaVersion.VERSION_21
      sourceCompatibility = JavaVersion.VERSION_21
      withSourcesJar()
   }

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
         jvmTarget = JvmTarget.JVM_21
         apiVersion = KotlinVersion.KOTLIN_2_2
         languageVersion = KotlinVersion.KOTLIN_2_2
         freeCompilerArgs.set(listOf("-Xwhen-guards"))
      }
   }
}
