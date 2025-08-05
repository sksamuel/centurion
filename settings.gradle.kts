rootProject.name = "centurion"

pluginManagement {
   repositories {
      mavenLocal()
      mavenCentral()
      maven("https://oss.sonatype.org/content/repositories/snapshots/")
      maven("https://plugins.gradle.org/m2/")
   }
   plugins {
      kotlin("jvm").version("2.2.0")
   }
}

enableFeaturePreview("STABLE_CONFIGURATION_CACHE")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")


include("centurion-avro")
//include("centurion-avro-lettuce")
//include("centurion-orc")
//include("centurion-parquet")
//include("centurion-schemas")

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

         val kotest = "6.0.0.M10"
         library("kotest-junit5", "io.kotest:kotest-runner-junit5:$kotest")
         library("kotest-core", "io.kotest:kotest-assertions-core:$kotest")
         library("kotest-json", "io.kotest:kotest-assertions-json:$kotest")
         library("kotest-property", "io.kotest:kotest-property:$kotest")
         library("kotest-testcontainers", "io.kotest:kotest-extensions-testcontainers:$kotest")

         library("kotlinx-coroutines-core", "org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")

         library("jackson-module-kotlin", "com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")

         library("lettuce-core", "io.lettuce:lettuce-core:6.7.1.RELEASE")

         library("commons-pool", "org.apache.commons:commons-pool2:2.12.1")

         val jmh = "1.37"
         library("jmh-core", "org.openjdk.jmh:jmh-core:$jmh")
         library("jmh-generator-annprocess", "org.openjdk.jmh:jmh-generator-annprocess:$jmh")

         val testcontainers = "1.21.1"
         library("testcontainers", "org.testcontainers:testcontainers:$testcontainers")
         library("testcontainers-redis", "com.redis:testcontainers-redis:2.2.4")

         bundle(
            "testing",
            listOf(
               "kotest-junit5",
               "kotest-core",
               "kotest-json",
               "kotest-property",
               "kotest-testcontainers",
            )
         )
      }
   }
}
