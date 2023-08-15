ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.13.8"

val latisVersion  = "ea2ab6c"
val circeVersion  = "0.14.5"
val http4sVersion = "0.23.23"

lazy val hapi = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "latis3-hapi",
    libraryDependencies ++= Seq(
      "com.github.latis-data.latis3" %% "latis3-core"         % latisVersion,
      "org.http4s"                   %% "http4s-ember-client" % http4sVersion,
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
    "com.typesafe"   % "config"              % "1.4.2",
    "org.scalameta" %% "munit"               % "0.7.29" % Test,
    "org.typelevel" %% "munit-cats-effect-3" % "1.0.7" % Test
  ),
  scalacOptions -= "-Xfatal-warnings"
)
