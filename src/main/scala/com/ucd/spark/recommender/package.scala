package com.ucd.spark

import breeze.linalg.DenseVector

/**
  * Created by paukelly on 20/12/2016.
  */
package object recommender {

  implicit class RichVectorI(v: DenseVector[Int]) {
    def asDouble : DenseVector[Double] =
      for((index, data) <- v.pairs) yield { data.toDouble}
  }

  implicit class RichArray(v: Array[Boolean]) {
    def asInt : Array[Int] = v.map(b => if (b) 1 else 0)
    def asDouble : Array[Double] = v.map(b => if (b) 1.0 else 0.0)
  }

}
