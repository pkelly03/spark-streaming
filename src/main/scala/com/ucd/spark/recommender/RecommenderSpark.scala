package com.ucd.spark.recommender

import breeze.linalg._
import breeze.linalg.NumericOps.Arrays._
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

  def sessionHandler(userId: String, seedItemId: String) = {

    val relatedItems = recRelatedItems
      .select(explode($"related_items").as("related_item_id"))
      .where($"item_id" equalTo seedItemId)
      .as[String]
      .collect :+ seedItemId

    val userInfo = users
      .select($"item_ids", $"mentions", $"polarity_ratio")
      .where($"user_id" equalTo userId)
      .as[UserInfo]
      .head

    val SentimentThreshold = 0.7
    val CompellingThreshold = 0.5

    val itemsList: Seq[Dataset[Item]] = relatedItems.map(relItemId => {
      items
        .select($"item_id", $"opinion_ratio", $"star", $"item_name", $"related_items", $"average_rating", $"polarity_ratio", $"mentions")
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

    def betterProScores = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val betterProScoresFunc = udf { betterCount: Seq[Double] =>
          val sessionLength = itemsList.size.toDouble - 1
          (DenseVector(betterCount.toArray) / sessionLength).toArray
        }
        item.withColumn("better_pro_scores", betterProScoresFunc('better_count.as[Seq[Double]])).as[Item]
      }
    }

    def worseConScores = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val worseConScoresFunc = udf { worseCount: Seq[Double] =>
          val sessionLength = itemsList.size.toDouble - 1
          (DenseVector(worseCount.toArray) / sessionLength).toArray
        }
        item.withColumn("worse_cons_scores", worseConScoresFunc('worse_count.as[Seq[Double]])).as[Item]
      }
    }

    def pros = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val prosFunc = udf { (betterProScores: Seq[Double], polarityRatio: Seq[Double]) =>
          val userMentionsGreaterThanZero = userInfo.mentions :> DenseVector.zeros[Double](4).toArray
          val targetItemSentimentGreatherThanThreshold = polarityRatio.toArray :> DenseVector.fill[Double](4, SentimentThreshold).toArray
          betterProScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentGreatherThanThreshold :& userMentionsGreaterThanZero
        }
        item.withColumn("pros", prosFunc('better_pro_scores.as[Seq[Double]],'polarity_ratio.as[Seq[Double]])).as[Item]
      }
    }

    def cons = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val consFunc = udf { (worseCScores: Seq[Double], polarityRatio: Seq[Double]) =>
          val userMentionsGreaterThanZero = userInfo.mentions :> DenseVector.zeros[Double](4).toArray
          val targetItemSentimentLessThanOrEqualToThreshold = polarityRatio.toArray :<= DenseVector.fill[Double](4, SentimentThreshold).toArray
          worseCScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentLessThanOrEqualToThreshold :& userMentionsGreaterThanZero
        }
        item.withColumn("cons", consFunc('worse_cons_scores.as[Seq[Double]],'polarity_ratio.as[Seq[Double]])).as[Item]
      }
    }

    def betterProScoresSum = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val betterProScoresSumFunc = udf { (betterProScores: Seq[Double], pros: Seq[Boolean]) =>
          DenseVector(betterProScores.toArray).dot(DenseVector(pros.toArray.asDouble))
        }
        item.withColumn("better_pro_scores_sum", betterProScoresSumFunc('better_pro_scores.as[Seq[Double]],'pros.as[Seq[Double]])).as[Item]
      }
    }

    def worseConScoresSum = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val betterProScoresSumFunc = udf { (worseConsScores: Seq[Double], cons: Seq[Boolean]) =>
          DenseVector(worseConsScores.toArray).dot(DenseVector(cons.toArray.asDouble))
        }
        item.withColumn("worse_con_scores_sum", betterProScoresSumFunc('worse_cons_scores.as[Seq[Double]],'cons.as[Seq[Double]])).as[Item]
      }
    }

    def isSeed = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val isSeedFunc = udf { itemId: String =>
          itemId == seedItemId
        }
        item.withColumn("is_seed", isSeedFunc('item_id)).as[Item]
      }
    }

    def strength = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val strengthFunc = udf { (betterProScoresSum: Double, worseConsScoresSum: Double) =>
          betterProScoresSum - worseConsScoresSum
        }
        item.withColumn("strength", strengthFunc('better_pro_scores_sum,'worse_con_scores_sum)).as[Item]
      }
    }

    def betterThanCompellingThreshold(scores: Array[Double]): Array[Boolean] = {
      scores :> DenseVector.fill[Double](4, CompellingThreshold).toArray
    }

    def prosComp = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val prosCompFunc = udf { (pros: Seq[Boolean], betterProScores: Seq[Double]) =>
          pros.toArray :& betterThanCompellingThreshold(betterProScores.toArray)
        }
        item.withColumn("pros_comp", prosCompFunc('pros.as[Seq[Boolean]],'better_pro_scores.as[Seq[Double]])).as[Item]
      }
    }

    def consComp = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val consCompFunc = udf { (cons: Seq[Boolean], worseConScores: Seq[Double]) =>
          cons.toArray :& betterThanCompellingThreshold(worseConScores.toArray)
        }
        item.withColumn("cons_comp", consCompFunc('cons.as[Seq[Boolean]],'worse_cons_scores.as[Seq[Double]])).as[Item]
      }
    }

    def proNonZerosCount = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val proNonZerosCountFunc = udf { pros: Seq[Boolean] =>
          pros.toArray.countNonZeros
        }
        item.withColumn("pro_non_zeros_count", proNonZerosCountFunc('pros.as[Seq[Boolean]])).as[Item]
      }
    }

    def consNonZerosCount = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val consNonZerosCountFunc = udf { cons: Seq[Boolean] =>
          cons.toArray.countNonZeros
        }
        item.withColumn("cons_non_zeros_count", consNonZerosCountFunc('cons.as[Seq[Boolean]])).as[Item]
      }
    }

    def proCompNonZerosCount = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val proCompNonZerosCountFunc = udf { prosComp: Seq[Boolean] =>
          prosComp.toArray.countNonZeros
        }
        item.withColumn("pro_comp_non_zeros_count", proCompNonZerosCountFunc('pros_comp.as[Seq[Boolean]])).as[Item]
      }
    }

    def consCompNonZerosCount = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val consCompNonZerosCountFunc = udf { consComp: Seq[Boolean] =>
          consComp.toArray.countNonZeros
        }
        item.withColumn("cons_comp_non_zeros_count", consCompNonZerosCountFunc('cons_comp.as[Seq[Boolean]])).as[Item]
      }
    }

    def isComp = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val isCompFunc = udf { (proCompNonZerosCount: Int, consCompNonZerosCount: Int) =>
          proCompNonZerosCount > 0 || consCompNonZerosCount > 0
        }
        item.withColumn("is_comp", isCompFunc('pro_comp_non_zeros_count,'cons_comp_non_zeros_count)).as[Item]
      }
    }

    def betterAverage = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val betterAverageFunc = udf { (betterProScoresSum: Double, proCompNonZerosCount: Int) =>
          if (proCompNonZerosCount == 0) 0.0 else betterProScoresSum / proCompNonZerosCount
        }
        item.withColumn("better_average", betterAverageFunc('better_pro_scores_sum,'pro_comp_non_zeros_count)).as[Item]
      }
    }

    def worseAverage = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val worseAverageFunc = udf { (worseConScoresSum: Double, consCompNonZerosCount: Int) =>
          if (consCompNonZerosCount == 0) 0.0 else worseConScoresSum / consCompNonZerosCount
        }
        item.withColumn("worse_average", worseAverageFunc('worse_con_scores_sum,'cons_comp_non_zeros_count)).as[Item]
      }
    }

    def betterProScoresCompSum = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val betterProScoresCompSumFunc = udf { (betterProsScoresV: Seq[Double], prosComp: Seq[Boolean]) =>
          DenseVector(betterProsScoresV.toArray).dot(DenseVector(prosComp.toArray.asDouble))
        }
        item.withColumn("better_pro_scores_comp_sum", betterProScoresCompSumFunc('better_pro_scores,'pros_comp)).as[Item]
      }
    }

    def worseConScoresCompSum = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val worseConScoresCompSumFunc = udf { (worseConScoresV: Seq[Double], consComp: Seq[Boolean]) =>
          DenseVector(worseConScoresV.toArray).dot(DenseVector(consComp.toArray.asDouble))
        }
        item.withColumn("worse_con_scores_comp_sum", worseConScoresCompSumFunc('worse_cons_scores,'cons_comp)).as[Item]
      }
    }

    def betterAverageComp = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val betterAverageCompFunc = udf { (proCompNonZerosCount: Int, betterProScoresCompSum: Double) =>
          if (proCompNonZerosCount == 0) 0.0 else betterProScoresCompSum / proCompNonZerosCount
        }
        item.withColumn("better_average_comp", betterAverageCompFunc('pro_comp_non_zeros_count,'better_pro_scores_comp_sum)).as[Item]
      }
    }

    def worseAverageComp = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val worseAverageCompFunc = udf { (consCompNonZerosCount: Int, worseConScoresCompSum: Double) =>
          if (consCompNonZerosCount == 0) 0.0 else worseConScoresCompSum / consCompNonZerosCount
        }
        item.withColumn("worse_average_comp", worseAverageCompFunc('cons_comp_non_zeros_count,'worse_con_scores_comp_sum)).as[Item]
      }
    }

    itemsList.foreach { item =>
      val pipeline = betterThanCount | worseThanCount | betterProScores | worseConScores | pros | cons | betterProScoresSum |
        worseConScoresSum | isSeed | strength | prosComp | consComp | proNonZerosCount | consNonZerosCount |
        proCompNonZerosCount | consCompNonZerosCount | isComp | betterAverage | worseAverage | betterProScoresCompSum |
        worseConScoresCompSum | betterAverageComp | worseAverageComp

      pipeline.apply(item).show(10)
    }
  }

  private def compareAgainstAlternativeSentimentUsingOperator(targetItemSentiment: Array[Double], alternativeSentiment: List[Array[Double]], op: String): List[Array[Int]] = {
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