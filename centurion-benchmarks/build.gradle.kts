plugins {
   id("me.champeau.jmh") version "0.7.3"
   id("kotlin-conventions")
}

dependencies {
   jmh(projects.centurionAvro)
   jmh(libs.avro)
   jmh(libs.jackson.module.kotlin)
}

jmh {
   jmhVersion = "1.37"
   fork = 2
   warmupIterations = 3
   iterations = 5
   threads = 1
   resultFormat = "JSON"
   resultsFile = layout.buildDirectory.file("results/jmh/results.json")
   (project.findProperty("jmh.includes") as String?)
      ?.takeIf { it.isNotBlank() }
      ?.let { includes = listOf(it) }
}
