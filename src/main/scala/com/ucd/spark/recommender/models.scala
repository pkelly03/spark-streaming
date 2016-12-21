package com.ucd.spark.recommender

object models {

  case class Item(opinion_ratio: Array[Double], star: Double, item_name: String,
                  related_items: Array[String], average_rating: Double, polarity_ratio: Array[Double],
                  mentions: Array[Double])

  case class RelatedItems(related_items: Array[String], related_items_sims: Array[String])
  case class UserInfo(item_ids: Array[String], mentions: Array[Double], polarity_ratio: Array[Double])

  case class Explanation(explanation_id: String, user_id: String, session_id: String, seed_item_id: String, target_item_id: String,
                         target_item_mentions: Array[Double], target_item_sentiment: Array[Double], better_count: Array[Int], worse_count: Array[Int],
                         better_pro_scores: Array[Double], worse_con_scores: Array[Double], is_seed: Boolean, pros: Array[Boolean], cons: Array[Boolean],
                         n_pros: Int, n_cons: Int, strength: Double, pros_comp: Array[Boolean], cons_comp: Array[Boolean],
                         n_pros_comp: Int, n_cons_comp: Int, is_comp: Boolean, better_avg: Double, worse_avg: Double,
                         better_avg_comp: Double, worse_avg_comp: Double, strength_comp: Double) {

    def generateReport(): String = {

      s"""---- GENERATED EXPLANATION START FOR $target_item_id----
          |
            |explanationId : $explanation_id
          |userId : $user_id
          |sessionId : $session_id
          |seedItemId : $seed_item_id
          |targetItemId : $target_item_id
          |targetItemMentions : ${target_item_mentions.mkString(",")}
          |targetItemSentiment : ${target_item_sentiment.mkString(", ")}
          |betterCount : ${better_count.mkString(", ")}
          |worseCount : ${worse_count.mkString(", ")}
          |betterProScores : ${better_pro_scores.mkString(", ")}
          |worseConScores : ${worse_con_scores.mkString(", ")}
          |isSeed : $is_seed
          |pros : ${pros.mkString(", ")}
          |cons : ${cons.mkString(", ")}
          |nPros : $n_pros
          |nCons : $n_cons
          |strength : $strength
          |prosComp : $pros_comp
          |consComp : $cons_comp
          |proCompNonZerosCount : $n_pros_comp
          |consCompNonZerosCount : $n_cons_comp
          |isComp : $is_comp
          |betterAverage : $better_avg
          |worseAverage : $worse_avg
          |betterAverageComp : $better_avg_comp
          |worseAverageComp : $worse_avg_comp
          |strengthComp : $strength_comp
          |
          |---- GENERATED EXPLANATION FINISH FOR $target_item_id----
          |
          |
          """.stripMargin
    }
  }
}

