package com.ucd.spark.recommender

import com.ucd.spark.recommender.DB.buildDataSet
import org.apache.spark.sql.{DataFrameReader, SparkSession}
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

  //  def writeDataSetToEs: Unit = {
  //    EsSparkSQL.saveToEs(buildDataSet.toDF, "beers/reviews")
  //  }

  val spark = SparkSession.builder.master("local").appName("spark-elastic-search").getOrCreate()

  import spark.implicits._

  val itemConfig = Map("es.read.field.as.array.include" -> "cons_pol,item_ids,mentions,opinion_ratio,polarity_ratio,pros_pol,senti_avg,related_items,related_items_sims")
  val userConfig = Map("es.read.field.as.array.include" -> "opinion_ratio,senti_avg,pros_pol,cons_pol,polarity_ratio,mentions,item_ids")

  // read items from schema
  val items = EsSparkSQL.esDF(spark, "ba:items/ba:items", itemConfig)

  // read users from schema
  val users = EsSparkSQL.esDF(spark, "ba:users/ba:users", userConfig)

  // query
  users
    .where($"user_id" equalTo "beerguzzlerxyz")
    .select(users.item_ids)

// df_users.where("user_id = 'matthoc116'").select(explode(df_users.item_ids).alias('seed_item_id'), "*").select(['item_ids', 'seed_item_id', 'user_id']).show()

  //  df_users_sample = df_users.sample(False, 5./df_users.count())
  //  print 'size of sample: ', df_users_sample.count()
  //  df_users_sample.select(['user_id', 'item_ids']).show()

  //  items.printSchema
  items.show

  //  items.select(items.)

  users.show

}








