scalaVersion := "2.11.8"

val akkaVersion = "2.4.10"
val sparkVersion = "2.0.0"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
//  "org.elasticsearch" % "elasticsearch-spark-20_2.11" % "5.0.0-beta1"
  "org.elasticsearch" % "elasticsearch-hadoop" % "5.0.0-beta1"
)

resolvers += "clojars" at "https://clojars.org/repo"
resolvers += "conjars" at "http://conjars.org/repo"


