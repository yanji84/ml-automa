lazy val common = Seq(
  version := "0.1.0",
  scalaVersion := "2.10.4",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % "1.5.1" % "provided",
    "org.apache.spark" %% "spark-streaming-twitter" % "1.5.0",
    "com.typesafe" % "config" % "1.3.0" % "provided"
  ),
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
     {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
     }
  }
)

lazy val twitter = (project in file(".")).
  settings(common: _*).
  settings(
    name := "twitter",
    mainClass in (Compile, run) := Some("twitter.Main"))

