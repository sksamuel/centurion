plugins {
   id("com.vanniktech.maven.publish")
}

group = "com.sksamuel.centurion"
version = Ci.version

mavenPublishing {
   publishToMavenCentral(automaticRelease = true)
   signAllPublications()
   pom {
      name.set("centurion")
      description.set("Kotlin Avro/Parquet Toolkit")
      url.set("http://www.github.com/sksamuel/centurion")

      scm {
         connection.set("scm:git:http://www.github.com/sksamuel/centurion/")
         developerConnection.set("scm:git:http://github.com/sksamuel/")
         url.set("http://www.github.com/sksamuel/centurion/")
      }

      licenses {
         license {
            name.set("The Apache 2.0 License")
            url.set("https://opensource.org/licenses/Apache-2.0")
         }
      }

      developers {
         developer {
            id.set("sksamuel")
            name.set("Stephen Samuel")
            email.set("sam@sksamuel.com")
         }
      }
   }
}
