ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.12.8"

//val latisVersion    = "3.0.0-SNAPSHOT"
val http4sVersion     = "0.20.13"

lazy val `latis3-core` = ProjectRef(file("../latis3"), "core")

lazy val hapi = (project in file("."))
  .dependsOn(`latis3-core`)
  .settings(commonSettings)
  .settings(
    name := "latis3-hapi",
    libraryDependencies ++= Seq(
      // "io.latis-data"           %% "latis-core"      % latisVersion,
      "org.http4s"             %% "http4s-blaze-client" % http4sVersion,
    )
  )
  
lazy val commonSettings = compilerFlags ++ Seq(
  libraryDependencies ++= Seq(
    "com.typesafe"    % "config"      % "1.3.4",
    "org.scalatest"  %% "scalatest"   % "3.0.5" % Test
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
    "-language:higherKinds",
    "-Ypartial-unification"
  ),
  Compile / compile / scalacOptions ++= Seq(
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard"
  )
)

