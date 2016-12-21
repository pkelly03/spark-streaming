package com.ucd.spark

import breeze.linalg.DenseVector

package object recommender {

  implicit class RichVectorI(v: DenseVector[Int]) {
    def asDouble : DenseVector[Double] =
      for((index, data) <- v.pairs) yield { data.toDouble}
  }

  implicit class RichArray(v: Array[Boolean]) {
    def asInt : Array[Int] = v.map(b => if (b) 1 else 0)
    def asDouble : Array[Double] = v.map(b => if (b) 1.0 else 0.0)
    def countNonZeros: Int = v.asDouble.count(c => c != 0)
  }

}
