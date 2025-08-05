import org.gradle.kotlin.dsl.`kotlin-dsl`

repositories {
   mavenCentral()
}

plugins {
   `kotlin-dsl`
}

dependencies {
   implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:2.2.0")
   implementation("com.vanniktech.maven.publish:com.vanniktech.maven.publish.gradle.plugin:0.34.0")
}
