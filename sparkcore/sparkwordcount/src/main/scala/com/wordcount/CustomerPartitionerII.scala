package com.wordcount

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

class CustomerPartitionerII(numParts:Int) extends Partitioner {

  //覆盖分区数
  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    /** "unhappy".substring(2) returns "happy"
    * "Harbison".substring(3) returns "bison"*/
    ckey.substring(ckey.length-1).toInt%numParts
  }
}

object CustomerPartitionerII {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("partittoner").setMaster("local[*]"))

    val data=sc.parallelize(List("aa.2","bb.2","cc.3","dd.3","ee.5"),5)

    val r = data.mapPartitionsWithIndex((index, items) => Iterator(index + ": [" + items.mkString(",") + "]")).collect()

    for (i <- r) println(i)

    val keys = data.map((_, 1)).partitionBy(new CustomerPartitionerII(5)).keys

    val r2 = keys.mapPartitionsWithIndex((index, items) => Iterator(index + ": [" + items.mkString(",") + "]")).collect()

    for (i <- r2) println(i)
  }
}
