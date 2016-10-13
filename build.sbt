val sparkVersion = "2.0.0"

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.10" % sparkVersion,
  "org.elasticsearch" % "elasticsearch-hadoop" % "5.0.0-beta1"
)

resolvers += "clojars" at "https://clojars.org/repo"
resolvers += "conjars" at "http://conjars.org/repo"

packAutoSettings

