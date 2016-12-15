package com.ucd.spark.recommender

import com.ucd.spark.recommender.Timer.time
import org.apache.spark.sql._
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by paukelly on 08/11/2016.
  */

object Timer {

  import scala.concurrent.duration._

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    println("STARTING aggregate")
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("ELAPSED time: " + Duration(t1 - t0, MILLISECONDS).toSeconds + " seconds")
    result
  }
}
object ExplanationsGroupBySingleCore extends App {

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("spark-elastic-search")
    .getOrCreate()
  import spark.implicits._

  val itemConfig = Map("es.read.field.as.array.include" -> "cons_pol,item_ids,mentions,opinion_ratio,polarity_ratio,pros_pol,senti_avg,related_items,related_items_sims")
  val userConfig = Map("es.read.field.as.array.include" -> "opinion_ratio,senti_avg,pros_pol,cons_pol,polarity_ratio,mentions,item_ids")
  val explanationsConfig = Map("es.read.field.as.array.include" -> "target_item_sentiment,pros,target_item_average_rating,worse_count,better_pro_scores,target_item_mentions,cons, worse_con_scores, better_count, cons_comp, pros_comp")

  val explanations: DataFrame = EsSparkSQL.esDF(spark, "ba:rec_tarelated_explanation/ba:rec_tarelated_explanation", explanationsConfig)

  time {
    explanations
    .groupBy($"User_id")
    .count
    .show
  }
}

object ExplanationsGroupUsingTwoExecutors extends App {

  val spark = SparkSession
    .builder
    .master("local[2]")
    .appName("spark-elastic-search-2-executors")
    .getOrCreate()
  import spark.implicits._

  val itemConfig = Map("es.read.field.as.array.include" -> "cons_pol,item_ids,mentions,opinion_ratio,polarity_ratio,pros_pol,senti_avg,related_items,related_items_sims")
  val userConfig = Map("es.read.field.as.array.include" -> "opinion_ratio,senti_avg,pros_pol,cons_pol,polarity_ratio,mentions,item_ids")
  val explanationsConfig = Map("es.read.field.as.array.include" -> "target_item_sentiment,pros,target_item_average_rating,worse_count,better_pro_scores,target_item_mentions,cons, worse_con_scores, better_count, cons_comp, pros_comp")

  val explanations: DataFrame = EsSparkSQL.esDF(spark, "ba:rec_tarelated_explanation/ba:rec_tarelated_explanation", explanationsConfig)

  time {
    explanations
      .groupBy($"User_id")
      .count
      .show
  }
}
