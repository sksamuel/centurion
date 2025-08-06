import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
   `java-library`
   kotlin("jvm")
   id("io.kotest")
}

group = Libs.org
version = Ci.version

java {
   toolchain {
      languageVersion.set(JavaLanguageVersion.of(17))
   }
   sourceCompatibility = JavaVersion.VERSION_17
   targetCompatibility = JavaVersion.VERSION_17
   withSourcesJar()
}

kotlin {
   compilerOptions {
      jvmTarget.set(JvmTarget.JVM_17)
      apiVersion.set(KotlinVersion.KOTLIN_2_2)
      languageVersion.set(KotlinVersion.KOTLIN_2_2)
      freeCompilerArgs.set(listOf("-Xwhen-guards"))
   }
}

dependencies {
   testImplementation("org.jetbrains.kotlin:kotlin-stdlib:2.1.21")
   testImplementation("io.kotest:kotest-framework-engine:6.0.0-LOCAL")
   testImplementation("io.kotest:kotest-assertions-core:6.0.0-LOCAL")
   testImplementation("io.kotest:kotest-assertions-json:6.0.0-LOCAL")
   testImplementation("io.kotest:kotest-property:6.0.0-LOCAL")
}
