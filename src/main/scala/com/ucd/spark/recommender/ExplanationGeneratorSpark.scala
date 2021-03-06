package com.ucd.spark.recommender

import breeze.linalg._
import breeze.linalg.NumericOps.Arrays._
import com.ucd.spark.recommender.models._
import org.apache.spark.sql.{Dataset, SparkSession, _}
import org.apache.spark.sql.functions.udf

object ExplanationGeneratorSpark {

  def generateExplanationsForUserAndItem(user: UserInfoSpark, spark: SparkSession, recRelatedItems: DataFrame, items: DataFrame) = {
    import spark.implicits._

    val relatedItems = recRelatedItems
      .select($"related_items", $"related_items_sims")
      .where($"item_id" equalTo user.item_id)
      .as[RelatedItems]

    val relatedItemsDs = relatedItems.head()
    val relatedItemIds = relatedItemsDs.related_items :+ user.item_id
    val relatedSims: Array[Double] = relatedItemsDs.related_items_sims :+ 1.0
    val relatedItemsAndSims = relatedItemIds.zip(relatedSims).toMap

    val SentimentThreshold = 0.7
    val CompellingThreshold = 0.5

    val itemsList: Seq[Dataset[Item]] = relatedItemIds.map(relItemId => {
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
        item.withColumn("worse_con_scores", worseConScoresFunc('worse_count.as[Seq[Double]])).as[Item]
      }
    }

    def pros = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val prosFunc = udf { (betterProScores: Seq[Double], polarityRatio: Seq[Double]) =>
          val userMentionsGreaterThanZero = user.mentions :> DenseVector.zeros[Double](4).toArray
          val targetItemSentimentGreatherThanThreshold = polarityRatio.toArray :> DenseVector.fill[Double](4, SentimentThreshold).toArray
          betterProScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentGreatherThanThreshold :& userMentionsGreaterThanZero
        }
        item.withColumn("pros", prosFunc('better_pro_scores.as[Seq[Double]],'polarity_ratio.as[Seq[Double]])).as[Item]
      }
    }

    def cons = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val consFunc = udf { (worseCScores: Seq[Double], polarityRatio: Seq[Double]) =>
          val userMentionsGreaterThanZero = user.mentions :> DenseVector.zeros[Double](4).toArray
          val targetItemSentimentLessThanOrEqualToThreshold = polarityRatio.toArray :<= DenseVector.fill[Double](4, SentimentThreshold).toArray
          worseCScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentLessThanOrEqualToThreshold :& userMentionsGreaterThanZero
        }
        item.withColumn("cons", consFunc('worse_con_scores.as[Seq[Double]],'polarity_ratio.as[Seq[Double]])).as[Item]
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
        item.withColumn("worse_con_scores_sum", betterProScoresSumFunc('worse_con_scores.as[Seq[Double]],'cons.as[Seq[Double]])).as[Item]
      }
    }

    def isSeed = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {

        val isSeedFunc = udf { itemId: String =>
          itemId == user.item_id
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
        item.withColumn("cons_comp", consCompFunc('cons.as[Seq[Boolean]],'worse_con_scores.as[Seq[Double]])).as[Item]
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
        item.withColumn("worse_con_scores_comp_sum", worseConScoresCompSumFunc('worse_con_scores,'cons_comp)).as[Item]
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

    def strengthComp = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val strengthCompFunc = udf { (betterProScoresCompSum: Double, worseConScoresCompSum: Double) =>
          betterProScoresCompSum - worseConScoresCompSum
        }
        item.withColumn("strength_comp", strengthCompFunc('better_pro_scores_comp_sum,'worse_con_scores_comp_sum)).as[Item]
      }
    }

    def recSim = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val recSimFunc = udf { itemId: String =>
          relatedItemsAndSims.getOrElse(itemId, 0.0)
        }
        item.withColumn("rec_sim", recSimFunc('item_id)).as[Item]
      }
    }

    def sessionId = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val explanationIdFunc = udf { targetItemId: String =>
          s"${user.user_id}#${user.item_id}"
        }
        item.withColumn("session_id", explanationIdFunc('item_id)).as[Item]
      }
    }

    def explanationId = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val explanationIdFunc = udf { (targetItemId: String, sessionId: String) =>
          s"$sessionId##$targetItemId"
        }
        item.withColumn("explanation_id", explanationIdFunc('item_id, 'session_id)).as[Item]
      }
    }

    def userIdStore = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val userIdStoreFunc = udf { targetItemId: String =>
          s"${user.user_id}"
        }
        item.withColumn("user_id", userIdStoreFunc('item_id)).as[Item]
      }
    }

    def seedItemStore = new Pipe[Item, Item] {
      def apply(item: Dataset[Item]): Dataset[Item] = {
        val seedItemStoreFunc = udf { targetItemId: String =>
          s"${user.item_id}"
        }
        item.withColumn("seed_item_id", seedItemStoreFunc('item_id)).as[Item]
      }
    }

    def toExplanation = new Pipe[Item, ExplanationSpark] {
      def apply(itemDs: Dataset[Item]): Dataset[ExplanationSpark] = {
        itemDs
          .select($"explanation_id", $"user_id", $"session_id", $"seed_item_id", $"item_id".alias("target_item_id"),
            $"mentions".alias("target_item_mentions"), $"polarity_ratio".alias("target_item_sentiment"), $"better_count", $"worse_count",
            $"better_pro_scores", $"worse_con_scores", $"is_seed", $"pros", $"cons", $"pro_non_zeros_count".alias("n_pros"),
            $"cons_non_zeros_count".alias("n_cons"), $"strength", $"pros_comp", $"cons_comp", $"pro_comp_non_zeros_count".alias("n_pros_comp"),
            $"cons_comp_non_zeros_count".alias("n_cons_comp"), $"is_comp", $"better_average".alias("better_avg"), $"worse_average".alias("worse_avg"),
            $"better_average_comp".alias("better_avg_comp"), $"worse_average_comp".alias("worse_avg_comp"), $"strength_comp",
            $"average_rating".alias("target_item_average_rating"), $"star".alias("target_item_star"), $"rec_sim", $"average_rating"
          )
          .as[ExplanationSpark]
      }
    }

    val partialExplanations = itemsList.flatMap { item =>
      val explanationPipeline = seedItemStore | betterThanCount | worseThanCount | betterProScores | worseConScores | pros | cons | betterProScoresSum |
        worseConScoresSum | isSeed | strength | prosComp | consComp | proNonZerosCount | consNonZerosCount |
        proCompNonZerosCount | consCompNonZerosCount | isComp | betterAverage | worseAverage | betterProScoresCompSum |
        worseConScoresCompSum | betterAverageComp | worseAverageComp | strengthComp | sessionId | explanationId | userIdStore | recSim

      val allCalculations = explanationPipeline.apply(item)
      val explanationsDs = toExplanation.apply(allCalculations)
      explanationsDs.collect()
    }

    val explanations: Seq[Explanation] = Ranking.enrichWithRankingSpark(partialExplanations)
    explanations
  }

  private def compareAgainstAlternativeSentimentUsingOperator(targetItemSentiment: Array[Double], alternativeSentiment: List[Array[Double]], op: String): List[Array[Int]] = {
    alternativeSentiment.map((alternative: Array[Double]) => {
      (op match {
        case "gt" => targetItemSentiment :> alternative
        case "lte" => targetItemSentiment :<= alternative
      }).map { b => if (b) 1 else 0 }
    })
  }
}

