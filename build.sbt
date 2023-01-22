ThisBuild / scalaVersion     := "2.13.10"
ThisBuild / version          := "0.0.1"

lazy val sample = (project in file("."))
  .settings(
    name := "DominanceQueries",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.2",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.2",
    libraryDependencies += "com.lihaoyi" %% "ujson" % "0.9.6"
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("org.typelevel.cats.**" -> "repackaged.org.typelevel.cats.@1").inAll,
  ShadeRule.rename("cats.**" -> "repackaged.cats.@1").inAll,
)