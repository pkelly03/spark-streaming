package com.ucd.spark.recommender

import breeze.linalg._
import com.ucd.spark.recommender.models.{Item, RelatedItem, RelatedItems, UserInfo}
import org.apache.spark.sql.functions.{explode, udf}
import org.apache.spark.sql.{SparkSession, _}
import org.elasticsearch.spark.sql.EsSparkSQL
import org.apache.spark.sql.functions.col

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
      .select(explode($"related_items").as("related_item_id"))
      .where($"item_id" equalTo itemId)
      .as[String]
      .collect

    val itemsList: Seq[Dataset[Item]] = relatedItems.map(relItemId => {
      items
        .select($"opinion_ratio", $"star", $"item_name", $"related_items", $"average_rating", $"polarity_ratio", $"mentions")
        .where($"item_id" equalTo relItemId)
        .as[Item]
    })

    val alternativeSentiment = itemsList.flatMap(item => item.select($"polarity_ratio").as[Array[Double]].collect)

    def betterThanCount = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val betterFunc = udf { polarityRatio: Seq[Double] =>
          val betterThanMatrix = DenseMatrix(compareAgainstAlternativeSentimentUsingOperator(polarityRatio.toArray, alternativeSentiment.toList, "gt"): _*)
          sum(betterThanMatrix(::, *)).inner.asDouble.toArray
        }
        item.withColumn("better_count", betterFunc('polarity_ratio.as[Seq[Double]])).as[Item]
      }
    }

    def worseThanCount = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val worseThanFunc = udf { polarityRatio: Seq[Double] =>
          val betterThanMatrix = DenseMatrix(compareAgainstAlternativeSentimentUsingOperator(polarityRatio.toArray, alternativeSentiment.toList, "lte"): _*)
          (sum(betterThanMatrix(::, *)) - 1).inner.asDouble.toArray
        }
        item.withColumn("worse_count", worseThanFunc('polarity_ratio.as[Seq[Double]])).as[Item]
      }
    }

    itemsList.foreach { item =>
      item.printSchema()
      val pipeline = betterThanCount | worseThanCount
      pipeline.apply(item).show(10)
    }
  }

  private def compareAgainstAlternativeSentimentUsingOperator(targetItemSentiment: Array[Double], alternativeSentiment: List[Array[Double]], op: String): List[Array[Int]] = {
    import breeze.linalg.NumericOps.Arrays._
    alternativeSentiment.map((alternative: Array[Double]) => {
      (op match {
        case "gt" => targetItemSentiment.:>(alternative)
        case "lte" => targetItemSentiment.:<=(alternative)
      }).map { b => if (b) 1 else 0 }
    })
  }

  sessionHandler("rudzud", "3587")
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