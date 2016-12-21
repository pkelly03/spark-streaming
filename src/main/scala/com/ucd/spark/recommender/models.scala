package com.ucd.spark.recommender

object models {

  case class Item(opinion_ratio: Array[Double], star: Double, item_name: String,
                  related_items: Array[String], average_rating: Double, polarity_ratio: Array[Double],
                  mentions: Array[Double])

  case class RelatedItems(related_items: Array[String], related_items_sims: Array[String])
  case class UserInfo(item_ids: Array[String], mentions: Array[Double], polarity_ratio: Array[Double])

  case class Explanation(explanationId: String, userId: String, sessionId: String, seedItemId: String, targetItemId: String,
                         targetItemMentions: Array[Double], targetItemSentiment: Array[Double], betterCount: Array[Int], worseCount: Array[Int],
                         betterProScores: Array[Double], worseConScores: Array[Double], isSeed: Boolean, pros: Array[Boolean], cons: Array[Boolean],
                         proNonZerosCount: Int, consNonZerosCount: Int, strength: Double, prosComp: Array[Boolean], consComp: Array[Boolean],
                         proCompNonZerosCount: Int, consCompNonZerosCount: Int, isComp: Boolean, betterAverage: Double, worseAverage: Double,
                         betterAverageComp: Double, worseAverageComp: Double, strengthComp: Double) {

    def generateReport(): String = {

      s"""---- GENERATED EXPLANATION START FOR $targetItemId----
          |
            |explanationId : $explanationId
          |userId : $userId
          |sessionId : $sessionId
          |seedItemId : $seedItemId
          |targetItemId : $targetItemId
          |targetItemMentions : ${targetItemMentions.mkString(",")}
          |targetItemSentiment : ${targetItemSentiment.mkString(", ")}
          |betterCount : ${betterCount.mkString(", ")}
          |worseCount : ${worseCount.mkString(", ")}
          |betterProScores : ${betterProScores.mkString(", ")}
          |worseConScores : ${worseConScores.mkString(", ")}
          |isSeed : $isSeed
          |pros : ${pros.mkString(", ")}
          |cons : ${cons.mkString(", ")}
          |nPros : $proNonZerosCount
          |nCons : $consNonZerosCount
          |strength : $strength
          |prosComp : $prosComp
          |consComp : $consComp
          |proCompNonZerosCount : $proCompNonZerosCount
          |consCompNonZerosCount : $consCompNonZerosCount
          |isComp : $isComp
          |betterAverage : $betterAverage
          |worseAverage : $worseAverage
          |betterAverageComp : $betterAverageComp
          |worseAverageComp : $worseAverageComp
          |strengthComp : $strengthComp
          |
          |---- GENERATED EXPLANATION FINISH FOR $targetItemId----
          |
          |
          """.stripMargin
    }
  }
}

