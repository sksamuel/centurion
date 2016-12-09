lazy val root = project.in(file("."))
  .settings(projectSettings: _*)
  .settings(releaseSettings: _*)
  .settings(dependencies: _*)

val akkaVersion = "2.4.14"

lazy val dependencies = Seq(
  libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test"
  )
)

lazy val releaseSettings = Seq(
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseCrossBuild := true,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
  pomExtra := (
    <url>https://github.com/sksamuel/akka-patterns</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:sksamuel/akka-patterns.git</url>
        <connection>scm:git@github.com:sksamuel/akka-patterns.git</connection>
      </scm>
      <developers>
        <developer>
          <id>sksamuel</id>
          <name>sksamuel</name>
          <url>http://github.com/akka-patterns</url>
        </developer>
      </developers>)
)

lazy val projectSettings = Seq(
  organization := "com.sksamuel.akka",
  name := "akka-patterns",
  scalaVersion := "2.11.8",
  crossScalaVersions := Seq("2.12.1", "2.11.8"),
  scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  fork in run := true,
  cancelable in Global := true,
  scalaVersion in ThisBuild := scalaVersion.value,
  parallelExecution in Test := false
)
