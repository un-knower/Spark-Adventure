package com.wordcount

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


class CustomerAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  private val _hashAcc = new mutable.HashMap[String, Int]()

  // 检测是否为空
  override def isZero: Boolean = {
    _hashAcc.isEmpty
  }

  // 拷贝一个新的累加器
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new CustomerAccumulator()
    _hashAcc.synchronized {
      newAcc._hashAcc ++= _hashAcc
    }
    newAcc
  }

  // 重置一个累加器
  override def reset(): Unit = {
    _hashAcc.clear()
  }

  // 每一个分区中用于添加数据的方法 小SUM
  override def add(v: String): Unit = {
    _hashAcc.get(v) match {
      case None => _hashAcc += ((v, 1))
      case Some(a) => _hashAcc += ((v, a + 1))
    }
  }

  // 合并每一个分区的输出 总sum
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case o: AccumulatorV2[String, mutable.HashMap[String, Int]] =>
        for ((k, v) <- o.value) {
          _hashAcc.get(k) match {
            case None => _hashAcc += ((k, v))
            case Some(a) => _hashAcc += ((k, a + v))
          }
        }
    }
  }

  // 输出值
  override def value: mutable.HashMap[String, Int] = {
    _hashAcc
  }
}

object CustomerAccumulator {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("partittoner").setMaster("local[*]"))

    val abc = "HIII"

    val hashAcc = new CustomerAccumulator()
    sc.register(hashAcc, "abc")

    val rdd = sc.makeRDD(Array("a", "b", "c", "a", "b", "c", "d"))

    rdd.foreach(hashAcc.add)

    for ((k, v) <- hashAcc.value) {
      println(" [" + k + ":" + v + "]")
    }

    sc.stop()
  }
}
