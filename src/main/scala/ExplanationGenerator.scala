package com.ucd.spark.recommender

import breeze.linalg.DenseMatrix
import com.ucd.spark.recommender.models.{Explanation, Item, UserInfo}
import breeze.linalg.{DenseVector, _}
import breeze.linalg.NumericOps.Arrays._

object ExplanationGenerator {

  def generateExplanation(userId: String, seedItemId: String, userInfo: UserInfo, sessionItemInfo: Map[String, Item],
                          relatedItemsAndSims: Map[String, Double]) = {

    import com.ucd.spark.recommender._

    val SentimentThreshold = 0.7
    val CompellingThreshold = 0.5
    val sessionId = s"$userId#$seedItemId"

    val alternativeSentiment = sessionItemInfo.values.map(item => item.polarity_ratio)

    val explanations = sessionItemInfo.keys.map { targetItemId =>
      val targetItemOpt = sessionItemInfo.get(targetItemId)

      targetItemOpt.map { targetItem =>

        val targetItemMentions = targetItem.mentions
        val betterThanMatrix = DenseMatrix(compareAgainstAlternativeSentimentUsingOperator(targetItem.polarity_ratio, alternativeSentiment.toList, "gt"): _*)
        val worseThanMatrix = DenseMatrix(compareAgainstAlternativeSentimentUsingOperator(targetItem.polarity_ratio, alternativeSentiment.toList, "lte"): _*)
        val betterCount = sum(betterThanMatrix(::, *))
        val worseCount = sum(worseThanMatrix(::, *)) - 1

        val sessionLength = sessionItemInfo.size.toDouble - 1
        val betterProScores = betterCount.inner.asDouble / sessionLength
        val worseConScores = worseCount.inner.asDouble / sessionLength

        val userMentionsGreaterThanZero = userInfo.mentions :> DenseVector.zeros[Double](4).toArray
        val targetItemSentimentGreatherThanThreshold = targetItem.polarity_ratio :> DenseVector.fill[Double](4, SentimentThreshold).toArray
        val targetItemSentimentLessThanOrEqualToThreshold = targetItem.polarity_ratio :<= DenseVector.fill[Double](4, SentimentThreshold).toArray
        val pros: Array[Boolean] = betterProScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentGreatherThanThreshold :& userMentionsGreaterThanZero
        val cons: Array[Boolean] = worseConScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentLessThanOrEqualToThreshold :& userMentionsGreaterThanZero

        val betterProScoresSum: Double = betterProScores.dot(DenseVector(pros.asDouble))
        val worseConScoresSum: Double = worseConScores.dot(DenseVector(cons.asDouble))

        val isSeed: Boolean = seedItemId == targetItemId

        val strength: Double = betterProScoresSum - worseConScoresSum

        def betterThanCompellingThreshold(scores: Array[Double]): Array[Boolean] = {
          scores :> DenseVector.fill[Double](4, CompellingThreshold).toArray
        }
        val prosComp: Array[Boolean] = pros :& betterThanCompellingThreshold(betterProScores.toArray)
        val consComp: Array[Boolean] = cons :& betterThanCompellingThreshold(worseConScores.toArray)

        val proNonZerosCount: Int = pros.countNonZeros
        val consNonZerosCount: Int = cons.countNonZeros

        val proCompNonZerosCount: Int = prosComp.countNonZeros
        val consCompNonZerosCount: Int = consComp.countNonZeros

        val isComp: Boolean = proCompNonZerosCount > 0 || consCompNonZerosCount > 0

        val betterAverage: Double = if (proCompNonZerosCount == 0) 0.0 else betterProScoresSum / proCompNonZerosCount
        val worseAverage: Double = if (consCompNonZerosCount == 0) 0.0 else worseConScoresSum / consCompNonZerosCount

        val betterProScoresCompSum: Double = betterProScores.dot(DenseVector(prosComp.asDouble))
        val worseConScoresCompSum: Double = worseConScores.dot(DenseVector(consComp.asDouble))

        val betterAverageComp: Double = if (proCompNonZerosCount == 0) 0.0 else betterProScoresCompSum / proCompNonZerosCount
        val worseAverageComp: Double = if (consCompNonZerosCount == 0) 0.0 else worseConScoresCompSum / consCompNonZerosCount

        val strengthComp: Double = betterProScoresCompSum - worseConScoresCompSum

        val explanationId: String = s"$sessionId##$targetItemId"

        val recSim: Double = relatedItemsAndSims.get(targetItemId).getOrElse(0)
        val averageRating: Double = sessionItemInfo.get(targetItemId).map(_.average_rating).getOrElse(0)

        Explanation(explanationId, userId, sessionId, seedItemId, targetItemId, targetItemMentions, targetItem.polarity_ratio,
          betterCount.inner.toArray, worseCount.inner.toArray, betterProScores.toArray, worseConScores.toArray, isSeed, pros, cons,
          proNonZerosCount, consNonZerosCount, strength, prosComp, consComp, proCompNonZerosCount, consCompNonZerosCount, isComp,
          betterAverage, worseAverage, betterAverageComp, worseAverageComp, strengthComp, targetItem.average_rating, targetItem.star,
          recSim, averageRating)
      }
    }
    explanations.toList.flatten
  }

  private def compareAgainstAlternativeSentimentUsingOperator(targetItemSentiment: Array[Double], alternativeSentiment: List[Array[Double]], op: String): List[Array[Int]] = {
    alternativeSentiment.map((alternative: Array[Double]) => {
      (op match {
        case "gt" => targetItemSentiment.:>(alternative)
        case "lte" => targetItemSentiment.:<=(alternative)
      }).map { b => if (b) 1 else 0 }
    })
  }


}
