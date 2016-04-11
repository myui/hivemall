import sbt._
import sbt.Keys._

object HivemallSparkBuild extends Build {

  lazy val hivemallspark = Project(
    id = "hivemall-spark",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "hivemall-spark",
      organization := "maropu",
      version := "0.0.6",
      scalaVersion := "2.10.4",
      javaOptions += "-Xmx8g"
      // add other settings here
    )
  )
}
