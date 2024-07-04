
lazy val root = project
  .in(file("."))
  .settings(
    name := "pg",
    version := "1.0",
    scalaVersion := "2.12.13",
    libraryDependencies ++= Seq(
         "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0",
         "org.scalameta"    %% "munit"      % "1.0.0" % Test,
         "org.apache.spark" %% "spark-sql"  % "3.5.1",
         "org.apache.spark" %% "spark-core"  % "3.5.1",
         "org.apache.logging.log4j" %% "log4j-api-scala" % "13.1.0"
    )
)
