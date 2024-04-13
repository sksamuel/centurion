rootProject.name = "centurion"

pluginManagement {
   repositories {
      mavenLocal()
      mavenCentral()
      maven {
         url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
      }
      maven {
         url = uri("https://plugins.gradle.org/m2/")
      }
   }
}

enableFeaturePreview("STABLE_CONFIGURATION_CACHE")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

include("centurion-arrow")
include("centurion-avro")
include("centurion-schemas")
include("centurion-orc")
include("centurion-parquet")
