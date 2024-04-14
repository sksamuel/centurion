rootProject.name = "centurion"

pluginManagement {
   repositories {
      mavenLocal()
      mavenCentral()
      maven("https://oss.sonatype.org/content/repositories/snapshots/")
      maven("https://plugins.gradle.org/m2/")
   }
}

enableFeaturePreview("STABLE_CONFIGURATION_CACHE")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

include("centurion-arrow")
include("centurion-avro")
include("centurion-orc")
include("centurion-parquet")
include("centurion-schemas")
