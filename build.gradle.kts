import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
   kotlin("jvm")
   id("signing")
}

allprojects {
   apply(plugin = "org.jetbrains.kotlin.jvm")

   group = Libs.org
   version = Ci.version

   java {
      targetCompatibility = JavaVersion.VERSION_17
      sourceCompatibility = JavaVersion.VERSION_17
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

   tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
      kotlinOptions.jvmTarget = "17"
      kotlinOptions.apiVersion = "1.8"
      kotlinOptions.languageVersion = "1.8"
   }
}
