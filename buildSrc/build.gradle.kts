repositories {
   mavenCentral()
   gradlePluginPortal()
   mavenLocal()
}

plugins {
   `kotlin-dsl`
}

dependencies {
   implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:2.2.0")
   implementation("io.kotest:io.kotest.gradle.plugin:6.0.0.M11")
   implementation("com.vanniktech.maven.publish:com.vanniktech.maven.publish.gradle.plugin:0.34.0")
}
