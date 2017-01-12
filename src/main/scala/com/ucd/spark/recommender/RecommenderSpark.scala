package com.ucd.spark.recommender

import com.ucd.spark.recommender.models._
import org.apache.spark.sql.functions.{explode, udf}
import org.apache.spark.sql.{SparkSession, _}
import org.elasticsearch.spark.sql.EsSparkSQL
import com.typesafe.scalalogging.StrictLogging
import com.ucd.spark.recommender.Timer.time

object RecommenderSpark extends App with StrictLogging {

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

  time {
    users
      .select($"user_id", explode($"item_ids").as("item_id"), $"mentions", $"polarity_ratio")
      .as[UserInfoSpark].collect.foreach { user =>
        logger.info(s"Generating explanation for user id : ${user.user_id}, and item id : ${user.item_id}")
        val explanations = ExplanationGeneratorSpark.generateExplanationsForUserAndItem(user, spark, recRelatedItems, items)
        val explanationsDS = spark.createDataset(explanations)
        EsSparkSQL.saveToEs(explanationsDS, "ba:rec_tarelated_explanation/ba:rec_tarelated_explanation", explanationsConfig)
        logger.info(s"Finished generating explanation for user id : ${user.user_id}, and item id : ${user.item_id}")
      }
  }
}


trait Pipe[In, Out] extends Serializable {
  def apply(rdd: Dataset[In]): Dataset[Out]

  def |[Final](next: Pipe[Out, Final]): Pipe[In, Final] = {
    // Close over outer object
    val self = this
    new Pipe[In, Final] {
      // Run first transform, pass results to next
      def apply(ds: Dataset[In]) = next(self(ds))
    }
  }
}