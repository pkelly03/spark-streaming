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

  def enrichWithRankingSpark(explanations: Seq[Explanation]): Seq[Explanation] = {

    lazy val explanationsWithTargetItemStar = ranking(explanations.map(_.target_item_star)).zipWithIndex.map {
      case (r, i) => explanations(i).copy(rank_target_item_star = Some(r))
    }

    lazy val explanationsWithTargetItemAverageRanking = ranking(explanationsWithTargetItemStar.map(_.target_item_average_rating)).zipWithIndex.map {
      case (r, i) => explanationsWithTargetItemStar(i).copy(rank_target_item_average_rating = Some(r))
    }

    lazy val explanationsWithAverageRatingRanking = ranking(explanationsWithTargetItemAverageRanking.map(_.average_rating)).zipWithIndex.map {
      case (r, i) => explanationsWithTargetItemAverageRanking(i).copy(rank_average_rating = Some(r))
    }

    lazy val explanationsWithRecSimRanking = ranking(explanationsWithAverageRatingRanking.map(_.rec_sim)).zipWithIndex.map {
      case (r, i) => explanationsWithAverageRatingRanking(i).copy(rank_rec_sim = Some(r))
    }

    lazy val explanationsWithStrengthRanking = ranking(explanationsWithRecSimRanking.map(_.strength)).zipWithIndex.map {
      case (r, i) => explanationsWithRecSimRanking(i).copy(rank_strength = Some(r))
    }

    lazy val explanationsWithStrengthCompRanking = ranking(explanationsWithStrengthRanking.map(_.strength_comp)).zipWithIndex.map {
      case (r, i) => explanationsWithStrengthRanking(i).copy(rank_strength_comp = Some(r))
    }
    explanationsWithStrengthCompRanking
  }

  def ranking(numbersToRank: Seq[Double]): Seq[Int] = {
    val ranking = numbersToRank.sortBy(x => -x).zip(Stream from 1).toMap
    numbersToRank.map(ranking)
  }
}
