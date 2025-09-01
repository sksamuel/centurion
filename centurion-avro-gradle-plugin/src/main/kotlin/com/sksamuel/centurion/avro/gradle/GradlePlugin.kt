package com.sksamuel.centurion.avro.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import kotlin.io.path.absolutePathString

abstract class GradlePlugin : Plugin<Project> {

   override fun apply(target: Project) {

      target.tasks.register("generateDataClasses", GenerateDataClassesTask::class.java) {
         it.group = "centurion"
         it.description = "Generate Kotlin data classes from Avro schemas"

         val outputBase = target.projectDir.toPath().resolve("src/main/kotlin")
         outputBase.toFile().mkdirs()
         it.output.set(outputBase.absolutePathString())
      }
   }
}
