val scala2Version = "2.12.13"

lazy val root = project
  .in(file("."))
  .settings(
    name := "mysources",
    version := "1.0",

    scalaVersion := scala2Version,

    libraryDependencies ++= Seq(
         "org.scalameta"    %% "munit"      % "1.0.0" % Test,
         "org.apache.spark" %% "spark-sql"  % "3.5.1",
         "org.apache.spark" %% "spark-core"  % "3.5.1",
    )
)
