ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.13.6"

val latisVersion  = "22d6cb6"
val circeVersion  = "0.14.1"
val http4sVersion = "0.23.1"

lazy val hapi = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "latis3-hapi",
    libraryDependencies ++= Seq(
      "com.github.latis-data.latis3" %% "latis3-core"         % latisVersion,
      "org.http4s"                   %% "http4s-blaze-client" % http4sVersion,
      "org.http4s"                   %% "http4s-circe"        % http4sVersion,
      "io.circe"                     %% "circe-core"          % circeVersion,
      "io.circe"                     %% "circe-parser"        % circeVersion
    ),
    resolvers ++= Seq(
      "jitpack" at "https://jitpack.io"
    )
  )
  
lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "com.typesafe"    % "config"      % "1.4.1",
    "org.scalatest"  %% "scalatest"   % "3.2.9" % Test
  ),
  scalacOptions -= "-Xfatal-warnings"
)
