package com.ucd.spark.recommender

import com.typesafe.scalalogging.StrictLogging
import com.ucd.spark.recommender.Timer.time
import org.apache.spark.sql._
import org.elasticsearch.spark.sql.EsSparkSQL

object Timer extends StrictLogging {

  import scala.concurrent.duration._

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    logger.info("Starting Generation of explanations")
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Finished Generation - ELAPSED time: " + Duration(t1 - t0, MILLISECONDS).toSeconds + " seconds")
    result
  }
}
