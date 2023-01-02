ThisBuild / scalaVersion     := "2.13.9"
ThisBuild / version          := "0.0.1"

lazy val sample = (project in file("."))
  .settings(
    name := "DominanceQueries",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
  )