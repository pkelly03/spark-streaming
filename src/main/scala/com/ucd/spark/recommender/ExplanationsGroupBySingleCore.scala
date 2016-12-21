package com.ucd.spark.recommender

import com.ucd.spark.recommender.Timer.time
import org.apache.spark.sql._
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * Created by paukelly on 08/11/2016.
  */

object Timer {

  import scala.concurrent.duration._

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    println("STARTING aggregate")
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("ELAPSED time: " + Duration(t1 - t0, MILLISECONDS).toSeconds + " seconds")
    result
  }
}
