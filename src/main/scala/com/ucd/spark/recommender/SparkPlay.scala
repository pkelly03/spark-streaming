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

//  val explanations: DataFrame = EsSparkSQL.esDF(spark, "ba:rec_tarelated_explanation/ba:rec_tarelated_explanation", explanationsConfig)

//  explanations.printSchema



  // select/where
//  users
//    .select($"user_id", $"item_ids")
//    .where($"user_id" equalTo "belgianbrown")
//    .show

  // select/explode
//  users
//    .select($"user_id", explode($"item_ids").as("items_ids_1"))
//    .where($"user_id" equalTo "belgianbrown")
//    .show

  // TODO: Get sample working
//  users
//    .sample(false, 20)
//    .select($"user_id", explode($"item_ids").as("items_ids_1"))
//    .show

  // with column
//  users
//    .withColumn("FlatType", explode($"polarity_ratio"))
//    .show


  //  find the average number of items that are reviewed by a user
//  explanations
//      .groupBy($"User_id")
//      .count
//      .show(20)


//  val x = DenseVector.zeros[Double](5)
//  println(x(0))
//  x(3 to 4) := .5
//  println(x)
//
//  val dm = DenseMatrix((1.0,2.0,3.0),
//    (4.0,5.0,6.0))
//
//  println(dm(::, *) + DenseVector(1.0, 4.0))
//
//  // mean
//  println(mean(dm(*, ::)))
//
//  // distributions
//  val expo = new Exponential(0.5)
//  println(breeze.stats.meanAndVariance(expo.samples.take(10000)))

  sessionHandler("ffejherb", "68616")
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
//    val itemInfo = itemInfoDs.head()
    val sessionId = s"$userId#$itemId"

    generateExplanationBase(userId,itemId, userInfo, itemInfo)
    println("session id : " + sessionId)
  }

  def generateExplanationBase(userId: String, sessionId: String, userInfo: UserInfo, sessionItemInfo: Map[String, Item]) = {

    import com.ucd.spark.recommender._

    val SentimentThreshold = 0.7
    val CompellingThreshold = 0.5
    val alternativeSentiment = sessionItemInfo.values.map(item => item.polarity_ratio)
    println(alternativeSentiment)

    sessionItemInfo.keys.map { targetItemId =>
      val targetItemOpt = sessionItemInfo.get(targetItemId)

      targetItemOpt.map { targetItem =>

        val betterThanMatrix = DenseMatrix(compareAgainstAlternativeSentimentUsingOperator(targetItem, alternativeSentiment.toList, "gt"): _*)
        val worseThanMatrix = DenseMatrix(compareAgainstAlternativeSentimentUsingOperator(targetItem, alternativeSentiment.toList, "lte"): _*)
        val betterThanCount = sum(betterThanMatrix(::, *))
        val worseThanCount = sum(worseThanMatrix(::, *)) - 1

        val sessionLength = sessionItemInfo.size.toDouble
        val betterProScores = betterThanCount.inner.asDouble / sessionLength
        val worseProScores = worseThanCount.inner.asDouble / sessionLength

        val userMentionsGreaterThanZero = userInfo.mentions :> DenseVector.zeros[Double](4).toArray
        val targetItemSentimentGreatherThanThreshold = targetItem.polarity_ratio :> DenseVector.fill[Double](4, SentimentThreshold).toArray
        val targetItemSentimentLessThanOrEqualToThreshold = targetItem.polarity_ratio :<= DenseVector.fill[Double](4, SentimentThreshold).toArray
        val pros = betterProScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentGreatherThanThreshold :& userMentionsGreaterThanZero
        val cons = worseProScores.toArray :> DenseVector.zeros[Double](4).toArray :& targetItemSentimentLessThanOrEqualToThreshold :& userMentionsGreaterThanZero

        val betterProScoresSum = betterProScores.dot(DenseVector(pros.asDouble))
        val worseProScoresSum = worseProScores.dot(DenseVector(cons.asDouble))

        val strength = betterProScoresSum - worseProScoresSum

        def betterThanCompellingThreshold(scores: Array[Double]): Array[Boolean] = {
          scores :> DenseVector.fill[Double](4, CompellingThreshold).toArray
        }
        val prosComp: Array[Boolean] = pros :& betterThanCompellingThreshold(betterProScores.toArray)
        val consComp: Array[Boolean] = cons :& betterThanCompellingThreshold(worseProScores.toArray)

        val proNonZerosCount = prosComp.countNonZeros
        val consNonZerosCount = consComp.countNonZeros
        val isComp = proNonZerosCount > 0 || consNonZerosCount > 0

        val betterAverage = if (proNonZerosCount == 0) 0.0 else betterProScoresSum / proNonZerosCount
        val worseAverage = if (consNonZerosCount == 0) 0.0 else worseProScoresSum / consNonZerosCount

        println(s"Score for target item : ${targetItem.item_name}")
        println(s"Better Pro Score Sum : $betterProScoresSum")
        println(s"Worse Pro Score Sum : $worseProScoresSum")
        println(s"Strength : $strength")



//        # normalise better scores to session_length
//        better_pro_scores = better_count.astype(float) / session_length
//        worse_con_scores = worse_count.astype(float) / session_length


      }
    }

  }



  def compareAgainstAlternativeSentimentUsingOperator(targetItem: Item, alternativeSentiment: List[Array[Double]], op: String): List[Array[Int]] = {
    alternativeSentiment.map((alternative: Array[Double]) => {
      (op match {
        case "gt" => targetItem.polarity_ratio.:>(alternative)
        case "lte" => targetItem.polarity_ratio.:<=(alternative)
      }).map { b => if (b) 1 else 0 }
    })
  }

  def getWorseCount(targetItem: Item, alternativeSentiment: List[Array[Double]]): List[Array[Int]] = {
    alternativeSentiment.map((alternative: Array[Double]) => {
      val bools = targetItem.polarity_ratio.:<=(alternative)
      bools.map { b => if (b) 1 else 0 }
    })
  }
}






