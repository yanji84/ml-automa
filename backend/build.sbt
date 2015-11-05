name := "ProjectX"
version := "1.0"
scalaVersion := "2.10.4"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.4.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0"
libraryDependencies += "com.rubiconproject.oss" % "jchronic" % "0.2.6"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
libraryDependencies += "commons-codec" % "commons-codec" % "1.10"
libraryDependencies += "org.scalanlp" % "breeze_2.10" % "0.11.2"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
		case PathList("META-INF", xs @ _*) => MergeStrategy.discard
		case x => MergeStrategy.first
	}
}