package com.ucd.spark.recommender
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
import breeze.linalg.{DenseVector, _}
import breeze.linalg.NumericOps.Arrays._
import com.ucd.spark.recommender.ExplanationGenerator.generateExplanation
import com.ucd.spark.recommender.models.{Explanation, Item, RelatedItems, UserInfo}

object RecommenderApp extends App {

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

    val relatedItemsDs = relatedItems.head()
    val relatedItemIds = relatedItemsDs.related_items :+ itemId
    val relatedSims: Array[Double] = relatedItemsDs.related_items_sims :+ 1.0

    val relatedItemsAndSims = relatedItemIds.zip(relatedSims).toMap

    val itemInfo = relatedItemIds.map(relItemId => {
      relItemId -> items
        .select($"item_id", $"opinion_ratio", $"star", $"item_name", $"related_items", $"average_rating", $"polarity_ratio", $"mentions")
        .where($"item_id" equalTo relItemId)
        .as[Item]
        .head
    }).toMap

    val fields = Seq("target_item_star", "target_item_average_rating")
    val explanations: List[Explanation] = generateExplanation(userId, itemId, userInfo, itemInfo, relatedItemsAndSims)

    val explanationsWithRanking: Seq[Explanation] = Ranking.enrichWithRanking(explanations)

    explanationsWithRanking.foreach(e => println(e.generateReport()))

    val explanationsDS = spark.createDataset(explanationsWithRanking)

    EsSparkSQL.saveToEs(explanationsDS, "ba:rec_tarelated_explanation/ba:rec_tarelated_explanation", explanationsConfig)
  }

  sessionHandler("rudzud", "3587")
}






