ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.12"
ThisBuild / organization := "com.malsolo"

val sparkCore = "org.apache.spark" %% "spark-core" % "3.0.1"

lazy val sparkCourse = (project in file("."))
  .settings(
    name := "Spark Course",
    libraryDependencies += sparkCore,
  )
