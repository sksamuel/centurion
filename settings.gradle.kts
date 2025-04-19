rootProject.name = "centurion"

pluginManagement {
   repositories {
      mavenLocal()
      mavenCentral()
      maven("https://oss.sonatype.org/content/repositories/snapshots/")
      maven("https://plugins.gradle.org/m2/")
   }
   plugins {
      kotlin("jvm").version("2.1.20")
   }
}

enableFeaturePreview("STABLE_CONFIGURATION_CACHE")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

include("centurion-arrow")
include("centurion-avro")
include("centurion-orc")
include("centurion-parquet")
include("centurion-schemas")

dependencyResolutionManagement {
   repositories {
      mavenCentral()
      mavenLocal()
      maven("https://oss.sonatype.org/content/repositories/snapshots")
      maven("https://s01.oss.sonatype.org/content/repositories/snapshots")
   }
   versionCatalogs {
      create("libs") {
         val tabby = "2.2.11"
         library("sksamuel-tabby", "com.sksamuel.tabby:tabby-fp:$tabby")

         library("avro", "org.apache.avro:avro:1.12.0")

         val kotest = "5.9.1"
         library("kotest-datatest", "io.kotest:kotest-framework-datatest:$kotest")
         library("kotest-junit5", "io.kotest:kotest-runner-junit5:$kotest")
         library("kotest-core", "io.kotest:kotest-assertions-core:$kotest")
         library("kotest-json", "io.kotest:kotest-assertions-json:$kotest")
         library("kotest-property", "io.kotest:kotest-property:$kotest")

         bundle(
            "testing",
            listOf(
               "kotest-datatest",
               "kotest-junit5",
               "kotest-core",
               "kotest-json",
               "kotest-property",
            )
         )
      }
   }
}
