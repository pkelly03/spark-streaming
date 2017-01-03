package com.ucd.spark.recommender

import com.ucd.spark.recommender.models.{RelatedItems, UserInfo}
import org.apache.spark.sql.{SparkSession, _}
import org.elasticsearch.spark.sql.EsSparkSQL

object RecommenderSpark extends App {

  val spark = SparkSession.builder.master("local").appName("spark-elastic-search").getOrCreate()

  import spark.implicits._

  val itemConfig = Map("es.read.field.as.array.include" -> "cons_pol,item_ids,mentions,opinion_ratio,polarity_ratio,pros_pol,senti_avg,related_items,related_items_sims")
  val userConfig = Map("es.read.field.as.array.include" -> "opinion_ratio,senti_avg,pros_pol,cons_pol,polarity_ratio,mentions,item_ids")
  val recRelatedConfig = Map("es.read.field.as.array.include" -> "related_items_sims,related_items")
  val explanationsConfig = Map("es.read.field.as.array.include" -> "target_item_sentiment,pros,target_item_average_rating,worse_count,better_pro_scores,target_item_mentions,cons, worse_con_scores, better_count, cons_comp, pros_comp")

  // read items from schema
  val items = EsSparkSQL.esDF(spark.sqlContext, "ba:items/ba:items", itemConfig)

  // read users from schema
  val users: DataFrame = EsSparkSQL.esDF(spark.sqlContext, "ba:users/ba:users", userConfig)

  val recRelatedItems: DataFrame = EsSparkSQL.esDF(spark.sqlContext, "ba:rec_tarelated/ba:rec_tarelated", recRelatedConfig)

  def sessionHandler(userId: String, itemId: String) = {
    val relatedItems = recRelatedItems
      .select($"related_items", $"related_items_sims")
      .where($"item_id" equalTo itemId)
      .as[RelatedItems]

    val userInfo = users
      .select($"item_ids", $"mentions", $"polarity_ratio")
      .where($"user_id" equalTo userId)
      .as[UserInfo]
      .head

    val betterCount = relatedItems.withColumn("betterCount", null)

  }
}