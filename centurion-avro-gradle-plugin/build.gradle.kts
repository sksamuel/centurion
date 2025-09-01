plugins {
   id("kotlin-conventions")
   id("publishing-conventions")
   id("java-gradle-plugin")
   id("com.gradle.plugin-publish").version("1.3.1")
}

dependencies {
   implementation(libs.kotlinpoet)
   implementation(libs.avro)
   compileOnly("org.jetbrains.kotlin:kotlin-gradle-plugin:2.2.10")
}

tasks {
   gradlePlugin {
      plugins {
         create("centurionAvroGradlePlugin") {
            id = "com.sksamuel.centurion.avro.gradle"
            implementationClass = "com.sksamuel.centurion.avro.gradle.GradlePlugin"
            displayName = "Centurion Avro Gradle Plugin"
            description = "Generate encoder, decoders and data classes from avro schemas"
         }
      }
   }
}
