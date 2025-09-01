package com.sksamuel.centurion.avro.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.provider.Property
import org.gradle.api.tasks.TaskAction
import java.nio.file.Paths
import kotlin.io.path.listDirectoryEntries

abstract class GenerateDataClassesTask : DefaultTask() {

   abstract val directory: Property<String>

   abstract val output: Property<String>

   @TaskAction
   fun generate() {
      Paths.get(directory.get()).listDirectoryEntries().forEach {

      }
   }
}

