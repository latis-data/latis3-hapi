ThisBuild / organization := "io.latis-data"
ThisBuild / scalaVersion := "2.13.5"

val latisVersion  = "f9c06477"
val circeVersion  = "0.13.0"
val http4sVersion = "0.23.0-RC1"

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
  
lazy val commonSettings = compilerFlags ++ Seq(
  libraryDependencies ++= Seq(
    "com.typesafe"    % "config"      % "1.3.4",
    "org.scalatest"  %% "scalatest"   % "3.2.9" % Test
  )
)

lazy val compilerFlags = Seq(
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "utf-8",
    "-feature",
    "-language:higherKinds"
  ),
  Compile / compile / scalacOptions ++= Seq(
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard"
  )
)

