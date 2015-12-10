name := "Jobserver"
version := "1.0"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.1" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.0" % "provided"
libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.6.0" % "provided"
libraryDependencies += "spark.jobserver" %% "job-server-extras" % "0.6.0" % "provided"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.2" % "provided"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
   {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
   }
}
