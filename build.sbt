scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"
val breezeVersion = "0.12"
val elasticSearchSparkVersion = "5.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.elasticsearch" %% "elasticsearch-spark-20" % elasticSearchSparkVersion,
//  "org.scalanlp" %% "breeze" % breezeVersion,
//  "org.scalanlp" %% "breeze-natives" % breezeVersion,
  "ch.qos.logback" % "logback-classic" % "1.1.8"
)

resolvers += "clojars" at "https://clojars.org/repo"
resolvers += "conjars" at "http://conjars.org/repo"

packAutoSettings

