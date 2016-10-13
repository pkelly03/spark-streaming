package com.ucd.spark.recommender

import com.ucd.spark.recommender.DB.buildDataSet
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql._

case class Beer(beerId: String, brewerId: String, abv: Double, style: String, appearance: Double, aroma: Double, palate: Double, taste: Double, overall: Double, profileName: String)

object DB {
  def buildDataSet = {
    val stCules = Beer("47986", "10325", 5.0, "Hefeweizen", 2.5, 2.0, 1.5, 1.5, 1.5, "stcules")
    val johnMichaelsen = Beer("47986", "10325", 7.7, "German Pilsener", 4.0, 4.5, 4.0, 4.5, 4.0, "johnmichaelsen")
    val redDiamond = Beer("10789", "1075", 7.2, "Oatmeal Stout", 2.5, 1.5, 2.5, 2.0, 2.0, "RedDiamond")
    List(stCules, johnMichaelsen, redDiamond)
  }
}

object RecommenderApp extends App {

  val spark = SparkSession.builder.master("local").appName("spark-elastic-search").getOrCreate()

  import spark.implicits._

  EsSparkSQL.saveToEs(buildDataSet.toDF, "beers/reviews")

  val beers = EsSparkSQL.esDF(spark, "beers/reviews")

  beers
    .filter($"overall" > 3)
    .show

}






