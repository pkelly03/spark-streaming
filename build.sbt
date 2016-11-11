val sparkVersion = "2.0.0"
val breezeVersion = "0.12"
val elasticSearchHadoopVersion: String = "5.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % sparkVersion exclude("org.slf4j","slf4j-log4j12") exclude("log4j","log4j"),
  "org.apache.spark" % "spark-sql_2.10" % sparkVersion exclude("org.slf4j","slf4j-log4j12") exclude("log4j","log4j"),
  "org.elasticsearch" % "elasticsearch-hadoop" % elasticSearchHadoopVersion,
  "org.scalanlp" %% "breeze" % breezeVersion,
  "org.scalanlp" %% "breeze-natives" % breezeVersion
)

resolvers += "clojars" at "https://clojars.org/repo"
resolvers += "conjars" at "http://conjars.org/repo"

packAutoSettings

