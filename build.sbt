ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "3.3.8"

val latisVersion  = "b66ec47"
val circeVersion  = "0.14.16"
val http4sVersion = "0.23.34"

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
    "com.typesafe"   % "config"            % "1.4.9",
    "org.scalameta" %% "munit"             % "1.3.3" % Test,
    "org.typelevel" %% "munit-cats-effect" % "2.2.0" % Test
  ),
  scalacOptions -= "-Xfatal-warnings"
)
