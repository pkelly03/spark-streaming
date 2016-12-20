package com.ucd.spark.recommender

import breeze.linalg.support.LiteralRow
import breeze.storage.Zero
import com.ucd.spark.recommender.DB.buildDataSet
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
//import breeze.linalg._
//import breeze.stats.mean
//import breeze.stats.distributions._
//import breeze.numerics._

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

  val explanations: DataFrame = EsSparkSQL.esDF(spark, "ba:rec_tarelated_explanation/ba:rec_tarelated_explanation", explanationsConfig)

  explanations.printSchema



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

    import breeze.linalg._
    import breeze.linalg.NumericOps.Arrays._

    val alternativeSentiment = sessionItemInfo.values.map(item => item.polarity_ratio)
    println(alternativeSentiment)

    sessionItemInfo.keys.map { targetItemId =>
      val targetItemOpt = sessionItemInfo.get(targetItemId)

      targetItemOpt.map { targetItem =>
        val better = alternativeSentiment.map(alternative => {
          val bools = targetItem.polarity_ratio.:>(alternative)
          bools.map { b => if (b) 1 else 0 }
        }).toList

        val betterThanDM = DenseMatrix(better: _*)
        val betterThanCount = sum(betterThanDM(::, *))
        println(betterThanCount)
//        DenseMatrix(x)
//        val x = (targetItemSentiment :> alternativeSentiment)
//        println(x)
      }
//      val targetItemSentiment = DenseVector(targetItem.)
    }

  }

  def dataFrameToCSVRowArray(dataset: Dataset[Row]): Array[String] = {
    dataset
      .collect()
      .map (row => {
        val rowString = row.toSeq.map {
          case b: Any => b.toString
          case _ => "null"
        }.mkString(",")

        // add only one new line character at the end of each row
        s"${rowString.stripLineEnd}\n"
      })
  }

  def dataSetToList(dataset: Dataset[Row]) = {
    val x = dataset.collect().foldLeft[List[String]](Nil)((acc, row) => row.mkString :: acc )
    x
  }
}






