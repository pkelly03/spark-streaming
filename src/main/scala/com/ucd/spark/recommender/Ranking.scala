package com.ucd.spark.recommender

import com.ucd.spark.recommender.models.{ExplanationSpark, Explanation}

object Ranking {

  def enrichWithRanking(explanations: Seq[Explanation]): Seq[Explanation] = {

    val explanationsWithTargetItemStar = ranking(explanations.map(_.target_item_star)).zipWithIndex.map {
      case (r, i) => explanations(i).copy(rank_target_item_star = Some(r))
    }

    val explanationsWithTargetItemAverageRanking = ranking(explanationsWithTargetItemStar.map(_.target_item_average_rating)).zipWithIndex.map {
      case (r, i) => explanationsWithTargetItemStar(i).copy(rank_target_item_average_rating = Some(r))
    }

    val explanationsWithAverageRatingRanking = ranking(explanationsWithTargetItemAverageRanking.map(_.average_rating)).zipWithIndex.map {
      case (r, i) => explanationsWithTargetItemAverageRanking(i).copy(rank_average_rating = Some(r))
    }

    val explanationsWithRecSimRanking = ranking(explanationsWithAverageRatingRanking.map(_.rec_sim)).zipWithIndex.map {
      case (r, i) => explanationsWithAverageRatingRanking(i).copy(rank_rec_sim = Some(r))
    }

    val explanationsWithStrengthRanking = ranking(explanationsWithRecSimRanking.map(_.strength)).zipWithIndex.map {
      case (r, i) => explanationsWithRecSimRanking(i).copy(rank_strength = Some(r))
    }

    val explanationsWithStrengthCompRanking = ranking(explanationsWithStrengthRanking.map(_.strength_comp)).zipWithIndex.map {
      case (r, i) => explanationsWithStrengthRanking(i).copy(rank_strength_comp = Some(r))
    }
    explanationsWithStrengthCompRanking
  }

  def enrichWithRankingSpark(partialExplanations: Seq[ExplanationSpark]): Seq[Explanation] = {

    def explanationsWithoutRanking(partialExplanations: Seq[ExplanationSpark]): Seq[Explanation] = partialExplanations.map(p => Explanation(p.explanation_id, p.user_id, p.session_id, p.seed_item_id, p.target_item_id, p.target_item_mentions,
      p.target_item_sentiment, p.better_count, p.worse_count, p.better_pro_scores, p.worse_con_scores, p.is_seed, p.pros, p.cons, p.n_pros, p.n_cons,
      p.strength, p.pros_comp, p.cons_comp, p.n_pros_comp, p.n_cons_comp, p.is_comp, p.better_avg, p.worse_avg, p.better_avg, p.worse_avg_comp,
      p.strength_comp, p.target_item_average_rating, p.target_item_star, p.rec_sim, p.average_rating)
    )

    def explanationsWithTargetItemStar(explanations: Seq[Explanation]) = ranking(explanations.map(_.target_item_star)).zipWithIndex.map {
      case (r, i) => explanations(i).copy(rank_target_item_star = Some(r))
    }

    def explanationsWithTargetItemAverageRanking(explanations: Seq[Explanation]) = ranking(explanations.map(_.target_item_average_rating)).zipWithIndex.map {
      case (r, i) => explanations(i).copy(rank_target_item_average_rating = Some(r))
    }

    def explanationsWithAverageRatingRanking(explanations: Seq[Explanation]) = ranking(explanations.map(_.average_rating)).zipWithIndex.map {
      case (r, i) => explanations(i).copy(rank_average_rating = Some(r))
    }

    def explanationsWithRecSimRanking(explanations: Seq[Explanation]) = ranking(explanations.map(_.rec_sim)).zipWithIndex.map {
      case (r, i) => explanations(i).copy(rank_rec_sim = Some(r))
    }

    def explanationsWithStrengthRanking(explanations: Seq[Explanation]) = ranking(explanations.map(_.strength)).zipWithIndex.map {
      case (r, i) => explanations(i).copy(rank_strength = Some(r))
    }

    def explanationsWithStrengthCompRanking(explanations: Seq[Explanation]) = ranking(explanations.map(_.strength_comp)).zipWithIndex.map {
      case (r, i) => explanations(i).copy(rank_strength_comp = Some(r))
    }

    val compose = explanationsWithoutRanking _ andThen explanationsWithTargetItemStar andThen explanationsWithTargetItemAverageRanking andThen explanationsWithAverageRatingRanking andThen
      explanationsWithRecSimRanking andThen explanationsWithStrengthRanking andThen explanationsWithStrengthCompRanking
    compose(partialExplanations)
  }

  def ranking(numbersToRank: Seq[Double]): Seq[Int] = {
    val ranking = numbersToRank.sortBy(x => -x).zip(Stream from 1).toMap
    numbersToRank.map(ranking)
  }
}
