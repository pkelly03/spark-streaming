package com.ucd.spark.recommender

import breeze.linalg.support.LiteralRow
import breeze.storage.Zero
import com.ucd.spark.recommender.DB.buildDataSet
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
import breeze.linalg.{DenseVector, _}
import breeze.linalg.NumericOps.Arrays._

case class Beer(beerId: String, brewerId: String, abv: Double, style: String, appearance: Double, aroma: Double, palate: Double, taste: Double, overall: Double, profileName: String)

object DB {
  def buildDataSet = {
    val stCules = Beer("47986", "10325", 5.0, "Hefeweizen", 2.5, 2.0, 1.5, 1.5, 1.5, "stcules")
    val johnMichaelsen = Beer("47986", "10325", 7.7, "German Pilsener", 4.0, 4.5, 4.0, 4.5, 4.0, "johnmichaelsen")
    val redDiamond = Beer("10789", "1075", 7.2, "Oatmeal Stout", 2.5, 1.5, 2.5, 2.0, 2.0, "RedDiamond")
    List(stCules, johnMichaelsen, redDiamond)
  }
}


object RecommenderApp extends App {

  //  def writeDataSetToEs: Unit = {
  //    EsSparkSQL.saveToEs(buildDataSet.toDF, "beers/reviews")
  //  }

  val spark = SparkSession.builder.master("local").appName("spark-elastic-search").getOrCreate()
  import spark.implicits._

  val itemConfig = Map("es.read.field.as.array.include" -> "cons_pol,item_ids,mentions,opinion_ratio,polarity_ratio,pros_pol,senti_avg,related_items,related_items_sims")
  val userConfig = Map("es.read.field.as.array.include" -> "opinion_ratio,senti_avg,pros_pol,cons_pol,polarity_ratio,mentions,item_ids")
  val recRelatedConfig = Map("es.read.field.as.array.include" -> "related_items_sims,related_items")
  val explanationsConfig = Map("es.read.field.as.array.include" -> "target_item_sentiment,pros,target_item_average_rating,worse_count,better_pro_scores,target_item_mentions,cons, worse_con_scores, better_count, cons_comp, pros_comp")

  // read items from schema
  val items = EsSparkSQL.esDF(spark, "ba:items/ba:items", itemConfig)

  // read users from schema
  val users: DataFrame = EsSparkSQL.esDF(spark, "ba:users/ba:users", userConfig)

  val recRelatedItems: DataFrame = EsSparkSQL.esDF(spark, "ba:rec_tarelated/ba:rec_tarelated", recRelatedConfig)

  case class Item(opinion_ratio: Array[Double], star: Double, item_name: String,
                  related_items: Array[String], average_rating: Double, polarity_ratio: Array[Double],
                  mentions: Array[Double])

  case class RelatedItems(related_items: Array[String], related_items_sims: Array[String])
  case class UserInfo(item_ids: Array[String], mentions: Array[Double], polarity_ratio: Array[Double])

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

    relatedItems.printSchema()
    relatedItems.show()

    val relatedItemsDs = relatedItems.head()
    val relatedItemIds = relatedItemsDs.related_items :+ itemId
    val relatedSims = relatedItemsDs.related_items_sims :+ 1

    val relatedItemsZipped = relatedItemIds.zip(relatedSims).toMap

    val itemInfo = relatedItemIds.map(relItemId => {
      println("Getting items for item : " + relItemId)
      relItemId -> items
        .select($"opinion_ratio", $"star", $"item_name", $"related_items", $"average_rating", $"polarity_ratio", $"mentions")
        .where($"item_id" equalTo relItemId)
        .as[Item]
        .head
    }).toMap
    generateExplanationBase(userId, itemId, userInfo, itemInfo)
  }

  def generateExplanationBase(userId: String, seedItemId: String, userInfo: UserInfo, sessionItemInfo: Map[String, Item]) = {

    import com.ucd.spark.recommender._

    val SentimentThreshold = 0.7
    val CompellingThreshold = 0.5
    val sessionId = s"$userId#$seedItemId"

    val alternativeSentiment = sessionItemInfo.values.map(item => item.polarity_ratio)

    sessionItemInfo.keys.map { targetItemId =>
      val targetItemOpt = sessionItemInfo.get(targetItemId)

      targetItemOpt.map { targetItem =>

        val targetItemMentions = targetItem.mentions
        val betterThanMatrix = DenseMatrix(compareAgainstAlternativeSentimentUsingOperator(targetItem.polarity_ratio, alternativeSentiment.toList, "gt"): _*)
        val worseThanMatrix = DenseMatrix(compareAgainstAlternativeSentimentUsingOperator(targetItem.polarity_ratio, alternativeSentiment.toList, "lte"): _*)
        val betterCount = sum(betterThanMatrix(::, *))
        val worseCount = sum(worseThanMatrix(::, *)) - 1

        val sessionLength = sessionItemInfo.size.toDouble
        val betterProScores = betterCount.inner.asDouble / sessionLength
        val worseConScores = worseCount.inner.asDouble / sessionLength

        val userMentionsGreaterThanZero = userInfo.mentions :> DenseVector.zeros[Double](4).toArray
        val targetItemSentimentGreatherThanThreshold = targetItem.polarity_ratio :> DenseVector.fill[Double](4, SentimentThreshold).toArray
        val targetItemSentimentLessThanOrEqualToThreshold = targetItem.polarity_ratio :<= DenseVector.fill[Double](4, SentimentThreshold).toArray
        val pros = betterProScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentGreatherThanThreshold :& userMentionsGreaterThanZero
        val cons = worseConScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentLessThanOrEqualToThreshold :& userMentionsGreaterThanZero

        val betterProScoresSum = betterProScores.dot(DenseVector(pros.asDouble))
        val worseConScoresSum = worseConScores.dot(DenseVector(cons.asDouble))

        val strength = betterProScoresSum - worseConScoresSum

        def betterThanCompellingThreshold(scores: Array[Double]): Array[Boolean] = {
          scores :> DenseVector.fill[Double](4, CompellingThreshold).toArray
        }
        val prosComp: Array[Boolean] = pros :& betterThanCompellingThreshold(betterProScores.toArray)
        val consComp: Array[Boolean] = cons :& betterThanCompellingThreshold(worseConScores.toArray)

        val proNonZerosCount = prosComp.countNonZeros
        val consNonZerosCount = consComp.countNonZeros
        val isComp = proNonZerosCount > 0 || consNonZerosCount > 0

        val betterAverage = if (proNonZerosCount == 0) 0.0 else betterProScoresSum / proNonZerosCount
        val worseAverage = if (consNonZerosCount == 0) 0.0 else worseConScoresSum / consNonZerosCount

        val betterProScoresCompSum = betterProScores.dot(DenseVector(prosComp.asDouble))
        val worseConScoresCompSum = worseConScores.dot(DenseVector(consComp.asDouble))

        val betterAvgComp = if (proNonZerosCount == 0) 0.0 else betterProScoresCompSum / proNonZerosCount
        val worseAvgComp = if (consNonZerosCount == 0) 0.0 else worseConScoresCompSum / consNonZerosCount

        val strengthComp = betterProScoresCompSum - worseConScoresCompSum

        val explanationId = s"$sessionId##$targetItemId"

        println(s"---- GENERATED EXPLANATION START FOR $targetItemId----")
        println(s"explanationId : $explanationId")
        println(s"userId : $userId")
        println(s"sessionId : $sessionId")
        println(s"seedItemId : $seedItemId")
        println(s"targetItemId : $targetItemId")
        println(s"targetItemMentions : $targetItemMentions")
        println(s"targetItemSentiment : ${targetItem.polarity_ratio}")
        println(s"betterCount : ${betterCount}")
        println(s"worseCount : ${worseCount}")
        println(s"betterProScores : ${betterProScores}")
        println(s"worseConScoresSum : ${worseConScoresSum}")

        println(s"Score for target item : ${targetItem.item_name}")
        println(s"Better Pro Score Sum : $betterProScoresSum")
        println(s"Worse Pro Score Sum : $worseConScoresSum")
        println(s"Strength : $strength")
        println(s"isComp : $isComp")
        println(s"betterAverage : $betterAverage")
        println(s"worseAverage : $worseAverage")
        println(s"betterProScoresCompSum : $betterProScoresCompSum")
        println(s"worseConScoresCompSum : $worseConScoresCompSum")
        println(s"betterAvgComp : $betterAvgComp")
        println(s"worseAvgComp : $worseAvgComp")
        println(s"strengthComp : $strengthComp")

        println(s"---- GENERATED EXPLANATION FINISH FOR $targetItemId----\n\n")

      }
    }
  }

  def compareAgainstAlternativeSentimentUsingOperator(targetItemSentiment: Array[Double], alternativeSentiment: List[Array[Double]], op: String): List[Array[Int]] = {
    alternativeSentiment.map((alternative: Array[Double]) => {
      (op match {
        case "gt" => targetItemSentiment.:>(alternative)
        case "lte" => targetItemSentiment.:<=(alternative)
      }).map { b => if (b) 1 else 0 }
    })
  }

  sessionHandler("ffejherb", "68616")

}






