package com.ucd.spark.recommender

object models {

  case class Item(opinion_ratio: Array[Double], star: Double, item_name: String,
                  related_items: Array[String], average_rating: Double, polarity_ratio: Array[Double],
                  mentions: Array[Double])

  case class ItemEnriched(item_id: String, opinion_ratio: Array[Double], star: Double, item_name: String, related_items: Array[String],
                          average_rating: Double, polarity_ratio: Array[Double], mentions: Array[Double], better_count: Array[Int],
                          worse_count: Array[Int], better_pro_scores: Array[Double], worse_cons_scores: Array[Double], pros: Array[Boolean],
                          cons: Array[Boolean],better_pro_scores_sum: Double,worse_con_scores_sum: Double,is_seed: Boolean, strength: Double,
                          pros_comp: Array[Boolean], cons_comp: Array[Boolean],pro_non_zeros_count: Int,cons_non_zeros_count: Int,
                          pro_comp_non_zeros_count: Int,cons_comp_non_zeros_count: Int, is_comp: Boolean, better_average: Double,
                          worse_average:Double, better_pro_scores_comp_sum: Double, worse_con_scores_comp_sum: Double, better_average_comp: Double,
                          worse_average_comp: Double, strength_comp: Double, sessionId: String, explanation_id: String, user_id: String, seed_item_id: String)

  case class RelatedItems(related_items: Array[String], related_items_sims: Array[Double])
  case class RelatedItem(related_item_id: String)
  case class UserInfo(item_id: Array[String], mentions: Array[Double], polarity_ratio: Array[Double])
  case class UserInfoSpark(user_id: String, item_id: String, mentions: Array[Double], polarity_ratio: Array[Double])

  case class ExplanationSpark(explanation_id: String, user_id: String, session_id: String, seed_item_id: String, target_item_id: String,
                         target_item_mentions: Array[Double], target_item_sentiment: Array[Double], better_count: Array[Double], worse_count: Array[Double],
                         better_pro_scores: Array[Double], worse_con_scores: Array[Double], is_seed: Boolean, pros: Array[Boolean], cons: Array[Boolean],
                         n_pros: Int, n_cons: Int, strength: Double, pros_comp: Array[Boolean], cons_comp: Array[Boolean],
                         n_pros_comp: Int, n_cons_comp: Int, is_comp: Boolean, better_avg: Double, worse_avg: Double,
                         better_avg_comp: Double, worse_avg_comp: Double, strength_comp: Double, target_item_average_rating: Double,
                         target_item_star: Double, rec_sim: Double, average_rating: Double)

  case class Explanation(explanation_id: String, user_id: String, session_id: String, seed_item_id: String, target_item_id: String,
                         target_item_mentions: Array[Double], target_item_sentiment: Array[Double], better_count: Array[Double], worse_count: Array[Double],
                         better_pro_scores: Array[Double], worse_con_scores: Array[Double], is_seed: Boolean, pros: Array[Boolean], cons: Array[Boolean],
                         n_pros: Int, n_cons: Int, strength: Double, pros_comp: Array[Boolean], cons_comp: Array[Boolean],
                         n_pros_comp: Int, n_cons_comp: Int, is_comp: Boolean, better_avg: Double, worse_avg: Double,
                         better_avg_comp: Double, worse_avg_comp: Double, strength_comp: Double, target_item_average_rating: Double,
                         target_item_star: Double, rec_sim: Double, average_rating: Double, rank_target_item_star: Option[Int] = None,
                         rank_target_item_average_rating: Option[Int] = None, rank_average_rating: Option[Int] = None, rank_rec_sim: Option[Int] = None,
                         rank_strength: Option[Int] = None, rank_strength_comp: Option[Int] = None
                        ) {

    def generateReport(): String = {

      s"""---- GENERATED EXPLANATION START FOR $target_item_id----
          |
          |explanation_id : $explanation_id
          |user_id : $user_id
          |session_id : $session_id
          |seed_item_id : $seed_item_id
          |target_item_id : $target_item_id
          |target_item_mentions : ${target_item_mentions.mkString(",")}
          |target_item_sentiment : ${target_item_sentiment.mkString(", ")}
          |better_count : ${better_count.mkString(", ")}
          |worse_count : ${worse_count.mkString(", ")}
          |better_pro_scores : ${better_pro_scores.mkString(", ")}
          |worse_con_scores : ${worse_con_scores.mkString(", ")}
          |is_seed : $is_seed
          |pros : ${pros.mkString(", ")}
          |cons : ${cons.mkString(", ")}
          |n_pros : $n_pros
          |n_cons : $n_cons
          |strength : $strength
          |pros_comp : ${pros_comp.mkString(", ")}
          |cons_comp : ${cons_comp.mkString(", ")}
          |n_pros_comp : $n_pros_comp
          |n_cons_comp : $n_cons_comp
          |is_comp : $is_comp
          |better_avg : $better_avg
          |worse_avg : $worse_avg
          |better_avg_comp : $better_avg_comp
          |worse_avg_comp : $worse_avg_comp
          |strength_comp : $strength_comp
          |target_item_average_rating : $target_item_average_rating
          |target_item_star : $target_item_star
          |average_rating : $average_rating
          |rec_sim : $rec_sim
          |rank_target_item_star : $rank_target_item_star
          |rank_target_item_average_rating : $rank_target_item_average_rating
          |rank_average_rating : $rank_average_rating
          |rank_rec_sim : $rank_rec_sim
          |rank_strength : $rank_strength
          |rank_strength_comp : $rank_strength_comp
          |
          |---- GENERATED EXPLANATION FINISH FOR $target_item_id----
          |
          |
          """.stripMargin
    }
  }
}

