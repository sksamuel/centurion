import org.gradle.api.tasks.testing.logging.TestExceptionFormat

buildscript {
   repositories {
      mavenLocal()
      mavenCentral()
      maven {
         url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
      }
   }
}

plugins {
   id("java")
   id("java-library")
   id("maven-publish")
   id("signing")
   kotlin("jvm").version(Libs.kotlinVersion)
}

allprojects {
   apply(plugin = "org.jetbrains.kotlin.jvm")

   group = Libs.org
   version = Ci.version

   java {
      targetCompatibility = JavaVersion.VERSION_11
      sourceCompatibility = JavaVersion.VERSION_11
      withSourcesJar()
   }

   repositories {
      mavenLocal()
      mavenCentral()
      maven { url = uri("https://oss.sonatype.org/content/repositories/snapshots") }
   }

   dependencies {
      testImplementation(Libs.Kotest.assertions)
      testImplementation(Libs.Kotest.junit5)
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
      target { this. }
   }

   tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
      kotlinOptions.jvmTarget = "1.8"
   }
}
